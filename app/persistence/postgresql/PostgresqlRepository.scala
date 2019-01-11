package persistence.postgresql

import javax.inject._
import Exceptions._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import config.AppExecutionContexts
import controllers.{ Formats, IndexDef }
import metrics.Metrics
import org.geolatte.geom.codec.{ Wkb, Wkt }
import org.geolatte.geom.{ Envelope, Geometry, Polygon }
import org.postgresql.util.PSQLException
import persistence._
import persistence.querylang.{ BooleanExpr, QueryParser }
import play.api.inject.ApplicationLifecycle
import play.api.libs.json._
import play.api.{ Configuration, Logger }
import slick.basic.DatabasePublisher
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import slick.jdbc.{ GetResult, PositionedResult }
import utilities.{ JsonUtils, Utils }
import GeoJsonFormats._
import scala.concurrent.Future
import scala.util.{ Success, Try }

@Singleton
class PostgresqlRepository @Inject() (
  configuration: Configuration,
  applicationLifecycle: ApplicationLifecycle,
  metrics: Metrics,
  implicit val mat: Materializer
)
    extends Repository with RepoHealth {

  import AppExecutionContexts.streamContext
  import utilities.Utils._

  private val geoJsonCol = "__geojson"

  private lazy val excludedSchemas: Option[Seq[String]] = configuration.getStringSeq("fs.exclude")

  private def mkDatabaseWithMetrics(db: Database): Database =
    db.source match {
      case source: HikariCPJdbcDataSource =>
        Logger.info(s"Setting DropWizard MetricRegistry on HikariCP data source")
        val ds = source.ds
        ds.setMetricRegistry(metrics.dropWizardMetricRegistry)
        db
      case _ => db
    }

  lazy val database = mkDatabaseWithMetrics(Database.forConfig("fs.postgresql"))

  applicationLifecycle.addStopHook(
    () => Utils.withInfo("Closing database") {
      Future.successful(database.close)
    }
  )

  case class Row(id: String, geometry: String, json: JsObject)

  def getRowResultFromTable(md: Metadata) = GetResult(tableRecordToRow(md))

  def tableRecordToRow(meta: Metadata) = (pr: PositionedResult) =>
    if (meta.jsonTable) Row(pr.nextString, pr.nextString(), Json.parse(pr.nextString()).asInstanceOf[JsObject])
    else {
      val rs = pr.rs
      val md = rs.getMetaData
      val id = rs.getString(meta.pkey)
      val geom = rs.getString(geoJsonCol)
      println(s"****************Geom value is : $geom")
      val props: Seq[(String, JsValue)] = for {
        idx <- 1 to pr.numColumns
        key = md.getColumnName(idx) if key != meta.geometryColumn && key != geoJsonCol && key != meta.pkey
        value = rs.getObject(idx)
      } yield (key, JsonUtils.toJsValue(value))
      val jsObj = Json.obj("id" -> id, "geometry" -> Wkt.fromWkt(geom), "properties" -> JsObject(props))
      Row(id, geom, jsObj)
    }

  implicit val getMetadataResult = GetResult(r => {
    val jsEnv = json(r.nextString())
    val env = Json.fromJson[Envelope](jsEnv) match {
      case JsSuccess(value, _) => value
      case _ => throw new RuntimeException(s"Invalid envelope JSON format. $jsEnv")
    }
    val indexLevel = r.nextInt
    val id_type = r.nextString
    val collection = r.nextString
    val geometryColumn = r.nextString
    val pkey = r.nextString
    if (geometryColumn == null)
      Metadata(collection, env, indexLevel, id_type)
    else // so this is a registered collection
      Metadata(collection, env, indexLevel, id_type, 0, geometryColumn, pkey, jsonTable = false)

  })

  var migrationsStatus: Option[Boolean] = None

  {
    import Utils._
    val result: Future[Boolean] = listDatabases.flatMap { dbs => sequence(dbs)(s => Migrations.executeOn(s)(database)) }
    result.onSuccess {
      case b => migrationsStatus = Some(b)
    }
  }

  override def createDb(dbname: String): Future[Boolean] = {
    val dbio = Sql.CREATE_SCHEMA(dbname) andThen Sql.CREATE_METADATA_TABLE_IN(dbname) andThen Sql.CREATE_VIEW_TABLE_IN(dbname)
    runOnDb("create-db")(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException(dbe) => throw dbe
      }
  }

  override def listDatabases: Future[List[String]] =
    database
      .run(Sql.LIST_SCHEMA)
      .map {
        _.toList
      }
      .recover {
        case MappableException(dbe) => throw dbe
      }

  override def dropDb(dbname: String): Future[Boolean] = {
    database
      .run(Sql.DROP_SCHEMA(dbname).transactionally)
      .map(_ => true)
      .recover {
        case MappableException(dbe) => throw dbe
      }
  }

  override def createCollection(dbName: String, colName: String, md: Metadata): Future[Boolean] = {
    val dbio = Sql.CREATE_COLLECTION_TABLE(dbName, colName) andThen
      Sql.INSERT_METADATA_JSON_COLLECTION(dbName, colName, md) andThen
      Sql.CREATE_COLLECTION_SPATIAL_INDEX(dbName, colName) andThen
      Sql.CREATE_COLLECTION_ID_INDEX(dbName, colName, md.idType)

    runOnDb("create-collection")(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException(dbe) => throw dbe
      }
  }

  override def registerCollection(db: String, collection: String, spec: Metadata): Future[Boolean] =
    withInfo(s"Starting with Registration of $db / $collection ") {

      def reg(db: String, meta: Metadata): Future[Boolean] = runOnDb("register-collection")(
        Sql.INSERT_METADATA_REGISTERED(db, collection, meta)
      ).map(_ => true)

      for {
        _ <- existsCollection(db, collection) flatMap { b =>
          if (b) Future.failed(CollectionAlreadyExistsException(s"Collection $db/$collection already exists"))
          else Future.successful(b)
        }
        (pkey, idType) <- getPrimaryKey(db, collection)
        cnt <- count(db, collection)
        finalMd = Metadata(collection, spec.envelope, spec.level, idType, cnt, spec.geometryColumn, pkey, jsonTable = false)
        result <- reg(db, finalMd)
      } yield result
    }

  override def listCollections(dbname: String): Future[List[String]] =
    runOnDb("list-collections")(Sql.SELECT_COLLECTION_NAMES(dbname))
      .map(_.toList)
      .recover {
        case MappableException(dbe) => throw dbe
      }

  /**
   * Retrieves the collection metadata from the server, but does not count number of rows
   *
   * @param db         the database
   * @param collection the collection (in fact table)
   * @return metadata, but row count is set to 0
   */
  def metadataFromDb(db: String, collection: String): Future[Metadata] =
    runOnDb("get-metadata")(Sql.SELECT_METADATA(db, collection)) map {
      case Some(v) => v
      case None => throw CollectionNotFoundException(s"Collection $db/$collection not found.")
    } recover {
      case MappableException(dbe) => throw dbe
    }

  override def metadata(database: String, collection: String, withCount: Boolean = false): Future[Metadata] = {
    val fCnt = if (withCount) count(database, collection) else Future.successful(-1L)
    fCnt.flatMap { cnt => metadataFromDb(database, collection).map(md => md.copy(count = cnt))
    }
  }

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] = {
    val dbio = Sql.DELETE_METADATA(dbName, colName) andThen
      Sql.DELETE_VIEWS_FOR_TABLE(dbName, colName) andThen
      Sql.DROP_TABLE(dbName, colName)
    runOnDb("delete-collection")(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException(dbe) => throw dbe
      }
  }

  override def count(db: String, collection: String): Future[Long] =
    runOnDb("count-items")(Sql.SELECT_COUNT(db, collection))
      .recover {
        case MappableException(dbe) => throw dbe
      }

  override def existsCollection(dbName: String, colName: String): Future[Boolean] =
    database.run(Sql.SELECT_COLLECTION_NAMES(dbName))
      .map(_.exists(_ == colName))
      .recover {
        case MappableException(dbe) => throw dbe
      }

  override def writer(db: String, collection: String): FeatureWriter = PGWriter(this, db, collection)

  //note that this may incur substantial overhead because of the parsing en modifying of Json values
  //TODO -- Explore if injecting the geojson geometry can be done in SQL
  def assemble(base: JsObject, ewkt: String): JsObject =
    (for {
      js <- Try {
        val geom = Wkt.fromWkt(ewkt)
        Json.toJson(geom)
      }.toOption
      gp = (__ \ "geometry").json.put(js)
      assembler = __.json.update(gp)
      transformed <- base.transform(assembler).asOpt
    } yield transformed).getOrElse(base)

  override def query(db: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
    limit: Option[Int] = None): Future[CountedQueryResult] = {

    val startMillis = System.currentTimeMillis()
    val projectingReads: Option[Reads[JsObject]] =
      for {
        ppl <- spatialQuery.projection
        withPrefix = ppl
          .withPrefix(List("type", "geometry", "id")) //TODO -- make sure that we use correct field names (can change for registered tables_
      } yield withPrefix.reads

    val from =
      if (!spatialQuery.explode)
        s"${quote(db)}.${quote(collection)}"
      else {
        val wq = Sql.windowFilterExpr(spatialQuery)
          .map(q => s" where $q").getOrElse("")
        s"(select id, (st_dump(geometry)).geom as geometry, json from ${quote(db)}.${quote(collection)} $wq ) xx"
      }

    //get the count
    lazy val fCnt: Future[Option[Long]] = if (spatialQuery.withCount) {
      val stmtTotal = Sql.SELECT_TOTAL_IN_QUERY(from, spatialQuery)
      runOnDb("get-query-total")(stmtTotal).map(Some(_))
    } else {
      Future.successful(None)
    }

    //get the data
    def project(js: JsObject): Option[JsObject] = projectingReads match {
      case None => Some(js)
      case Some(projections) => js.asOpt(projections)
    }

    val dataStmt = Sql.SELECT_DATA(from, spatialQuery, start, limit)

    val disableAutocommit = SimpleDBIO(_.connection.setAutoCommit(false))

    lazy val publisher: DatabasePublisher[JsObject] =
      database.stream(disableAutocommit
        andThen dataStmt.withStatementParameters(fetchSize = 256)).mapResult {
        case Row(_, geom, json) => assemble(project(json).get, geom)
      }

    def validate(spatialQuery: SpatialQuery): Future[Boolean] =
      if (spatialQuery.explode && !spatialQuery.metadata.jsonTable)
        Future.failed(new UnsupportedOperationException(s"EXPLODE parameter not supported on registered tables"))
      else
        Future.successful(true)

    for {
      _ <- validate(spatialQuery)
      cnt <- fCnt
    } yield (cnt, Source.fromPublisher(publisher).via(new MetricCalculator(startMillis, db, collection)))

  }

  override def delete(db: String, collection: String, query: BooleanExpr): Future[Boolean] =
    runOnDb("delete-data")(Sql.DELETE_DATA(db, collection, PGJsonQueryRenderer.render(query)(RenderContext("geometry")))) map { _ => true }

  def extractIdString(json: JsObject): String =
    (json \ "id").getOrElse(JsNull) match {
      case JsString(v) => s"'$v'"
      case JsNumber(i) => i.toString()
      case _ => throw new IllegalArgumentException("No ID property of type String or Number")
    }

  def batchInsert(db: String, collection: String, jsons: Seq[(JsObject, Geometry)]): Future[Int] = {
    val paramValues = jsons.map {
      case (json, geom) => (
        extractIdString(json),
        unescapeJson(stripGeometry(json)),
        org.geolatte.geom.codec.Wkb.toWkb(geom).toString
      )
    }
    val dbio = DBIO.sequence(paramValues.map { case (id, json, geom) => Sql.INSERT_DATA(db, collection, id, json, geom) }).map(_.sum)
    runOnDb("batch-insert")(dbio)
  }

  def update(db: String, collection: String, query: BooleanExpr, newValue: JsObject, geometry: Geometry): Future[Int] = {
    val whereExpr = PGJsonQueryRenderer.render(query)(RenderContext("geometry"))
    val stmt = Sql.UPDATE_DATA(db, collection, whereExpr, Json.stringify(newValue), Wkb.toWkb(geometry).toString)
    runOnDb("update")(stmt)
  }

  override def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int] = {
    val newGeometry = updateSpec.as[Geometry](GeoJsonFormats.geoJsonGeometryReads) //extract new geometry
    update(database, collection, query, updateSpec, newGeometry)
  }

  def upsert(db: String, collection: String, jsons: Seq[(JsObject, Geometry)]): Future[Int] = {
    val paramValues = jsons.map {
      case (json, geom) => (
        extractIdString(json),
        unescapeJson(stripGeometry(json)),
        org.geolatte.geom.codec.Wkb.toWkb(geom).toString
      )
    }
    val dbio = DBIO.sequence(paramValues.map { case (id, json, geom) => Sql.UPSERT_DATA(db, collection, id, json, geom) }).map(_.sum)
    runOnDb("batch-upsert")(dbio)
  }

  /**
   * Saves a view for the specified database and collection.
   *
   * @param db         the database for the view
   * @param collection the collection for the view
   * @param viewDef    the view definition
   * @return eventually true if this save resulted in the update of an existing view, false otherwise
   */
  override def saveView(db: String, collection: String, viewDef: JsObject): Future[Boolean] = {
    val viewName = (viewDef \ "name").as[String]

    getViewOpt(db, collection, viewName).flatMap {
      case Some(_) => dropView(db, collection, viewName)
      case _ => Future.successful(false)
    } flatMap { isOverWrite =>
      runOnDb("save-view")(Sql.INSERT_VIEW(db, collection, viewName, viewDef)) map {
        _ => isOverWrite
      }
    }
  }

  override def dropView(db: String, collection: String, id: String): Future[Boolean] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) runOnDb("drop-view")(Sql.DELETE_VIEW(db, collection, id)) map { _ => true }
      else throw CollectionNotFoundException()
    }

  override def getView(database: String, collection: String, id: String): Future[JsObject] =
    getViewOpt(database, collection, id).map { opt =>
      if (opt.isDefined) opt.get
      else throw ViewObjectNotFoundException()
    }

  def getViewOpt(db: String, collection: String, id: String): Future[Option[JsObject]] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) {
        runOnDb("get-view")(Sql.GET_VIEW(db, collection, id)) map {
          case Some(view) => view.asOpt[JsObject](Formats.ViewDefOut(db, collection))
          case _ => None
        }
      } else throw CollectionNotFoundException()
    }

  override def getViews(db: String, collection: String): Future[List[JsObject]] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) runOnDb("get-views")(Sql.GET_VIEWS(db, collection)) map {
        _.toList
      }
      else throw CollectionNotFoundException()
    }

  override def createIndex(dbName: String, colName: String, indexDef: IndexDef): Future[Boolean] = {
    val fRes = if (indexDef.regex) runOnDb("create-index")(
      Sql.CREATE_INDEX_WITH_TRGM(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
    )
    else runOnDb("create-index")(
      Sql.CREATE_INDEX(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
    )

    fRes map { _ => true } recover {
      case MappableException(dbe) => throw dbe
    }

  }

  private def toIndexDef(name: String, defText: String): Option[IndexDef] = {
    val pathElRegex = "\\'(\\w+)\\'".r
    val methodRegex = "USING (\\w+) ".r
    val castRegex = "::(\\w+)\\)\\)$".r

    val method = methodRegex.findFirstMatchIn(defText).map { m => m group 1 }
    val isForRegex = method.contains("gist")

    val cast = castRegex.findFirstMatchIn(defText).map { m => m group 1 } match {
      case Some("boolean") => "bool"
      case Some("numeric") => "decimal"
      case _ => "text"
    }

    val path = (for (m <- pathElRegex.findAllMatchIn(defText)) yield m group 1) mkString "."

    if (path.isEmpty) None //empty path, means not an index on JSON value (maybe spatial, maybe on ID
    else Some(IndexDef(name, path, cast, isForRegex)) //we can't determine the cast used during definition of index
  }

  private def getInternalIndices(dbName: String, colName: String): Future[List[IndexDef]] =
    existsCollection(dbName, colName).flatMap { exists =>
      if (exists) runOnDb("get-index")(Sql.SELECT_INDEXES_FOR_TABLE(dbName, colName)) map {
        seq =>
          seq.map(rd => toIndexDef(rd._1, rd._2)).collect {
            case Some(d) => d
          }.toList
      }
      else throw CollectionNotFoundException()
    }

  override def getIndices(dbName: String, colName: String): Future[List[String]] =
    getInternalIndices(dbName, colName).map(listIdx => listIdx.filter(q => q.path != "id").map(_.name))

  override def getIndex(dbName: String, colName: String, indexName: String): Future[IndexDef] =
    getInternalIndices(dbName, colName).map { listIdx =>
      listIdx.find(q => q.name == indexName) match {
        case Some(idf) => idf
        case None => throw IndexNotFoundException(s"Index $indexName on $dbName/$colName not found.")
      }
    }

  override def dropIndex(db: String, collection: String, index: String): Future[Boolean] =
    runOnDb("drop-index")(Sql.DROP_INDEX(db, collection, index)) map { _ => true }

  // RepoHealth implementation

  override def getActivityStats: Future[Vector[ActivityStats]] = runOnDb("pg_activity_stats") { Sql.SELECT_PG_STATS }
  override def getTableStats: Future[Vector[TableStats]] = runOnDb("pg_table_stats") { Sql.SELECT_PG_STAT_TABLES }

  //************************************************************************
  //Private Utility methods
  //************************************************************************

  def runOnDb[R](label: String)(dbio: DBIO[R]): Future[R] = {
    val startTimeNano = System.nanoTime()
    database.run(dbio).andThen {
      case _ =>
        metrics.prometheusMetrics.dbio.labels(label).observe((System.nanoTime - startTimeNano) / 10E6)
    }
  }

  //TODO -- fix that database.runs are "guarded"
  private def guardReady[T](block: => T): T = this.migrationsStatus match {
    case Some(b) => block
    case _ => throw NotReadyException("Busy migrating databases")
  }

  private def getPrimaryKey(db: String, coll: String): Future[(String, String)] = {
    type PKeyData = (String, String)

    def determinePkeyType(s: String): String = {
      val us = s.toLowerCase
      if (us.contains("text") || us.contains("char")) "text"
      else "decimal"
    }

    def toPKeyData(row: Option[(String, String)]): Option[PKeyData] = row map {
      case (fst, snd) => (fst, determinePkeyType(snd))
    }

    runOnDb("get-primary-key")(Sql.GET_TABLE_PRIMARY_KEY(db, coll)) map {
      toPKeyData
    } map {
      case Some(pk) => pk
      case None => throw InvalidPrimaryKeyException(s"Can't determine pkey configuration")
    }
  }

  //
  //    SQL Statements and utility functions.
  //
  //    Note that we cannot use prepared statements for DDL's

  //TODO -- should we check for quotes, spaces etc.  in str?
  // alternative is to use escapeSql in the org.apache.commons.lang.StringEscapeUtils
  private def quote(str: String): String = "\"" + str + "\""

  private def single_quote(str: String): String = "'" + str + "'"

  private def unescapeJson(js: JsObject): String = Json.stringify(js).replaceAll("'", "''")

  private val tr = (__ \ "geometry").json.prune
  private def stripGeometry(js: JsObject): JsObject = {
    js.transform(tr) match {
      case JsSuccess(pruned, _) => pruned
      case _ => js
    }
  }

  private def toColName(str: String): String = str.replaceAll("-", "_")

  private def schemaIsExcluded(collName: String): Boolean = excludedSchemas match {
    case Some(col) => col.contains(collName)
    case _ => false
  }

  private def withExcludeSchemas(clause: String, prop: String) = {
    def quoted(coll: Seq[String]) = coll.map(single_quote) mkString ","
    (clause, excludedSchemas) match {
      case (c, Some(coll)) if c.isEmpty => s" $prop not in ( ${quoted(coll)} )"
      case (c, Some(coll)) => s" $c AND $prop not in ( ${quoted(coll)} )"
      case _ => clause
    }
  }

  //These are the SQL statements for managing and retrieving data
  object Sql {

    import MetadataIdentifiers._

    val LIST_SCHEMA =
      sql"""
           select schema_name
           from information_schema.schemata
           where #${withExcludeSchemas("schema_owner = current_user", "schema_name")}
         """.as[String]

    private def fldSortSpecToSortExpr(spec: FldSortSpec): String = {
      //we use the #>> operator for 9.3 support, which extracts to text
      // if we move to 9.4  or later with jsonb then we can use the #> operator because jsonb are ordered
      s" json #>> '{${spec.fld.split("\\.") mkString ","}}' ${spec.direction.toString}"
    }

    def sort(query: SpatialQuery): String = query.metadata.jsonTable match {
      case true =>
        if (query.sort.isEmpty) "ID"
        else query.sort.map { arr => fldSortSpecToSortExpr(arr) } mkString ","
      case _ =>
        def colName(s: String): String = if (s.trim.startsWith("properties.")) s.trim.substring(11) else s.trim

        if (query.sort.isEmpty) query.metadata.pkey else query.sort.map(f => s"${colName(f.fld)} ${f.direction} ") mkString ","
    }

    def wktGeometry(query: SpatialQuery): Option[String] = {
      query.windowOpt.map(env => Wkt.toWkt(GeoJsonFormats.toPolygon(env)))
    }

    def windowFilterExpr(query: SpatialQuery): Option[String] = {
      val geomCol = if (query.metadata.jsonTable) "geometry" else query.metadata.geometryColumn
      def bboxIntersectionRenderer(wkt: String) = s"$geomCol && ${single_quote(wkt)}::geometry"
      val bboxGeom = wktGeometry(query)
      bboxGeom.map(bboxIntersectionRenderer)
    }

    def condition(query: SpatialQuery): Option[String] = {

      val renderer = if (query.metadata.jsonTable) PGJsonQueryRenderer else PGRegularQueryRenderer
      val geomCol = if (query.metadata.jsonTable) "geometry" else query.metadata.geometryColumn

      def geomIntersectionRenderer(wkt: String) = s"ST_Intersects($geomCol, ${single_quote(wkt)}::geometry)"
      val windowOpt = windowFilterExpr(query)
      val bboxGeom = wktGeometry(query: SpatialQuery)
      val intersectionOpt = query.intersectionGeometryWktOpt.map(geomIntersectionRenderer)
      implicit val renderContext = RenderContext(geomCol, bboxGeom) //set RenderContext
      val attOpt = query.queryOpt.map(renderer.render)

      (windowOpt ++ intersectionOpt ++ attOpt).reduceOption((condition1, condition2) => s"$condition1 and $condition2")
    }

    def SELECT_TOTAL_IN_QUERY(from: String, query: SpatialQuery): DBIO[Long] =
      sql"""
         SELECT COUNT(*)
         FROM #$from
         #${condition(query).map(c => s"WHERE $c").getOrElse("")}
     """.as[Long].map(_.head)

    def SELECT_DATA(from: String, query: SpatialQuery, start: Option[Int] = None, limit: Option[Int] = None) = {

      val cond = condition(query)

      val limitClause = limit match {
        case Some(lim) => s"\nLIMIT $lim"
        case _ => ""
      }

      val offsetClause = start match {
        case Some(s) => s"\nOFFSET $s"
        case _ => ""
      }

      val projection =
        if (query.metadata.jsonTable) "ID, ST_AsEWKT( geometry) as geom , json"
        else s" ${query.metadata.pkey}, ST_AsEWKT( ${query.metadata.geometryColumn}) as $geoJsonCol, * "

      sql"""
         SELECT #$projection
         FROM #$from
         #${cond.map(c => s"WHERE $c").getOrElse("")}
         ORDER BY #${sort(query)}
         #$offsetClause
         #$limitClause
     """.as[Row](getRowResultFromTable(query.metadata))

    }

    def UPDATE_DATA(db: String, col: String, where: String, json: String, geom: String) = {
      sqlu"""UPDATE #${quote(db)}.#${quote(col)}
          SET json = $json::json, geometry = $geom::geometry
          WHERE #$where
       """
    }

    def CREATE_SCHEMA(dbname: String) = sqlu"create schema #${quote(dbname)}"

    def DROP_SCHEMA(dbname: String) = sqlu"drop schema #${quote(dbname)} CASCADE"

    def CREATE_METADATA_TABLE_IN(dbname: String) =
      sqlu"""CREATE TABLE #${quote(dbname)}.#${quote(MetadataCollection)} (
              #${toColName(ExtentField)} JSON,
              #${toColName(IndexLevelField)} INT,
              #${toColName(IdTypeField)} VARCHAR(120),
              #${toColName(CollectionField)} VARCHAR(255) PRIMARY KEY,
              geometry_col VARCHAR(255),
              pkey VARCHAR(255)
              );
       """

    def CREATE_VIEW_TABLE_IN(dbname: String) =
      sqlu"""CREATE TABLE #${quote(dbname)}.#${quote(ViewCollection)} (
              COLLECTION VARCHAR,
              VIEW_NAME VARCHAR,
              VIEW_DEF JSON,
              UNIQUE (COLLECTION, VIEW_NAME)
              )
       """

    def CREATE_COLLECTION_TABLE(dbname: String, tableName: String) =
      sqlu"""CREATE TABLE #${quote(dbname)}.#${quote(tableName)} (
              id VARCHAR(255) PRIMARY KEY,
              geometry GEOMETRY,
              json JSON
              )
       """

    def GET_TABLE_PRIMARY_KEY(dbname: String, tableName: String): DBIO[Option[(String, String)]] = {
      val relname = s"$dbname.$tableName"
      sql"""
         SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
         FROM   pg_index i
         JOIN   pg_attribute a ON a.attrelid = i.indrelid
                              AND a.attnum = ANY(i.indkey)
         WHERE  i.indrelid = ${single_quote(relname)}::regclass
         AND    i.indisprimary;
       """.as[(String, String)].map(_.headOption)
    }

    def CREATE_COLLECTION_ID_INDEX(dbname: String, tableName: String, idType: String) =
      sqlu"""CREATE INDEX #${quote("idx_" + tableName + "_id")}
             ON #${quote(dbname)}.#${quote(tableName)} ( (json_extract_path_text(json, 'id')::#$idType) )
     """

    def CREATE_COLLECTION_SPATIAL_INDEX(dbname: String, tableName: String) =
      sqlu"""CREATE INDEX #${quote(tableName + "_spatial_index")}
              ON #${quote(dbname)}.#${quote(tableName)} USING GIST ( geometry )

     """

    def INSERT_DATA(dbname: String, tableName: String, id: String, json: String, geometry: String) =
      sqlu"""INSERT INTO #${quote(dbname)}.#${quote(tableName)}  (id, json, geometry)
             VALUES ($id, $json::json, $geometry::geometry)
       """

    def UPSERT_DATA(dbname: String, tableName: String, id: String, json: String, geometry: String) =
      sqlu"""INSERT INTO #${quote(dbname)}.#${quote(tableName)}  (id, json, geometry)
             VALUES ($id, $json::json, $geometry::geometry)
             ON CONFLICT (id) DO UPDATE SET json = EXCLUDED.json, geometry = EXCLUDED.geometry;
       """

    def DELETE_DATA(dbname: String, tableName: String, where: String) =
      sqlu"""DELETE FROM #${quote(dbname)}.#${quote(tableName)}
        WHERE #$where
     """

    def LIST_TABLE_NAMES(dbname: String) = {
      sqlu""" select table_name from information_schema.tables
              where
              table_schema = ${single_quote(dbname)}
              and table_type = 'BASE TABLE'
              and table_name != ${quote(MetadataCollection)}
       """
    }

    def INSERT_METADATA_JSON_COLLECTION(dbname: String, tableName: String, md: Metadata) =
      sqlu"""insert into #${quote(dbname)}.#${quote(MetadataCollection)} values(
              ${Json.stringify(Json.toJson(md.envelope))}::json,
              ${md.level},
              ${md.idType},
              $tableName
              )
     """

    def INSERT_METADATA_REGISTERED(dbname: String, tableName: String, md: Metadata) =
      sqlu"""insert into #${quote(dbname)}.#${quote(MetadataCollection)} values(
              ${single_quote(Json.stringify(Json.toJson(md.envelope)))}::json,
              ${md.level},
              ${single_quote(md.idType)},
              ${single_quote(tableName)},
              ${single_quote(md.geometryColumn)},
              ${single_quote(md.pkey)}
              )
     """

    def SELECT_COLLECTION_NAMES(dbname: String): DBIO[Seq[String]] =
      sql"""select #$CollectionField
        from  #${quote(dbname)}.#${quote(MetadataCollection)}
     """.as[String]

    def SELECT_COUNT(dbname: String, tablename: String): DBIO[Long] =
      sql"select count(*) from #${quote(dbname)}.#${quote(tablename)}".as[Long].map(_.head)

    def SELECT_METADATA(dbname: String, tablename: String): DBIO[Option[Metadata]] =
      sql"""select *
            from #${quote(dbname)}.#${quote(MetadataCollection)}
            where #$CollectionField = $tablename
      """.as[Metadata].map(_.headOption)

    def DELETE_METADATA(dbname: String, tablename: String) =
      sqlu"""delete
             from #${quote(dbname)}.#${quote(MetadataCollection)}
             where #$CollectionField = $tablename
     """

    def DROP_TABLE(dbname: String, tablename: String) =
      sqlu"drop table #${quote(dbname)}.#${quote(tablename)}"

    def DELETE_VIEWS_FOR_TABLE(dbname: String, tablename: String) =
      sqlu"""
         DELETE FROM #${quote(dbname)}.#${quote(ViewCollection)}
         WHERE COLLECTION = $tablename
     """

    def INSERT_VIEW(dbname: String, tableName: String, viewName: String, json: JsObject) =
      sqlu"""
         INSERT INTO #${quote(dbname)}.#${quote(ViewCollection)} VALUES (
           $tableName,
           $viewName,
           ${Json.stringify(json)}::json
         )
     """

    def DELETE_VIEW(dbname: String, tableName: String, viewName: String) =
      sqlu"""
         DELETE FROM #${quote(dbname)}.#${quote(ViewCollection)}
         WHERE COLLECTION = $tableName and VIEW_NAME = $viewName
     """

    implicit val getJson = GetResult(r => Json.parse(r.nextString()).asInstanceOf[JsObject])

    def GET_VIEW(dbname: String, coll: String, viewName: String) =
      sql"""
         SELECT VIEW_DEF
         FROM #${quote(dbname)}.#${quote(ViewCollection)}
         WHERE COLLECTION = $coll AND VIEW_NAME = $viewName
     """.as[JsObject].map {
        _.headOption
      }

    def GET_VIEWS(dbname: String, coll: String) = {
      sql"""
         SELECT VIEW_DEF
         FROM #${quote(dbname)}.#${quote(ViewCollection)}
         WHERE COLLECTION = $coll
     """.as[JsObject]
    }

    //    def CHECK_TRGM_EXTENSION() =
    //    s"""
    //       select true from pg_extension where extname = 'pg_trgm'
    //     """.stripMargin

    def CREATE_INDEX(dbName: String, colName: String, indexName: String, path: String, cast: String) = {
      val pathExp = path.split("\\.").map(
        el => single_quote(el)
      ).mkString(",")
      sqlu"""CREATE INDEX #${quote(indexName)}
             ON #${quote(dbName)}.#${quote(colName)} ( (json_extract_path_text(json, #$pathExp)::#$cast) )
      """
    }

    def CREATE_INDEX_WITH_TRGM(dbName: String, colName: String, indexName: String, path: String, cast: String) = {
      val pathExp = path.split("\\.").map(
        el => single_quote(el)
      ).mkString(",")
      sqlu"""CREATE INDEX #${quote(indexName)}
             ON #${quote(dbName)}.#${quote(colName)} using gist
             ( (json_extract_path_text(json, #$pathExp)::#$cast) gist_trgm_ops)
      """
    }

    def SELECT_INDEXES_FOR_TABLE(db: String, col: String) =
      sql"""
         SELECT INDEXNAME, INDEXDEF
         FROM pg_indexes
         WHERE schemaname = $db AND tablename = $col
     """.as[(String, String)]

    def DROP_INDEX(db: String, col: String, indexName: String) =
      sqlu"""
         DROP INDEX IF EXISTS #${quote(db)}.#${quote(indexName)}
     """

    implicit val getTableStatsResult = GetResult(r => TableStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<,
      r.<<, r.<<, r.<<, r.<<))

    implicit val getResultActivityStats = GetResult(r => ActivityStats(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    val SELECT_PG_STATS =
      sql"""
           SELECT pid, application_name, xact_start, query_start,
                  wait_event_type, wait_event, state, query
           FROM pg_stat_activity
         """.as[ActivityStats]

    val SELECT_PG_STAT_TABLES =
      sql"""
            SELECT schemaname, relname, n_live_tup, n_dead_tup, n_mod_since_analyze,
                   last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, vacuum_count,
                   autovacuum_count, analyze_count, autoanalyze_count
            FROM pg_stat_user_tables
        """.as[TableStats]

  }

  object MappableException {

    def getStatus(dbe: PSQLException): String = dbe.getServerErrorMessage.getSQLState

    def getMessage(dbe: PSQLException): String = dbe.getServerErrorMessage.getMessage

    def unapply(t: Throwable): Option[RuntimeException] = t match {
      case t: PSQLException if getStatus(t).contains("42P06") =>
        Some(DatabaseAlreadyExistsException(getMessage(t)))
      case t: PSQLException if getStatus(t).contains("42P07") =>
        Some(CollectionAlreadyExistsException(getMessage(t)))
      case t: PSQLException if getStatus(t).contains("3F000") =>
        Some(DatabaseNotFoundException(getMessage(t)))
      case t: PSQLException if getStatus(t).contains("42P01") =>
        Some(CollectionNotFoundException(getMessage(t)))
      case _ => None
    }
  }

  class MetricCalculator(startMillis: Long, db: String, coll: String) extends GraphStage[FlowShape[JsObject, JsObject]] {

    val in = Inlet[JsObject]("object.in")
    val out = Outlet[JsObject]("object.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var cnt = 0
      var started = false

      def time = System.currentTimeMillis() - startMillis

      setHandler(out, new OutHandler {
        override def onPull() = {
          if (started) ()
          else {
            started = true
            metrics.prometheusMetrics.dbioStreamStart.labels(db, coll).observe(time)
          }
          pull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush() = {
          cnt = cnt + 1
          push(out, grab(in))
        }

        override def onUpstreamFinish(): Unit = {
          metrics.prometheusMetrics.dbioStreamComplete.labels(db, coll).observe(time)
          metrics.prometheusMetrics.numObjectsRetrieved.labels(db, coll).observe(cnt)
          completeStage()
        }
      })

    }
  }
}
