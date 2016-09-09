package featureserver.postgresql


import javax.inject._

import Exceptions._
import config.{AppExecutionContexts, ConfigurationValues}
import controllers.{Formats, IndexDef}
import featureserver._
import featureserver.json.GeometryReaders
import org.geolatte.geom.codec.{Wkb, Wkt}
import org.geolatte.geom.{ByteBuffer, Envelope, Geometry, Polygon}
import org.postgresql.util.PSQLException
import play.api.libs.iteratee.Iteratee
import play.api.libs.json._
import querylang.{BooleanExpr, QueryParser}
import utilities.{JsonHelper, JsonUtils, Utils}
import slick.jdbc.PostgresProfile.api._
import play.api.libs.streams.Streams
import play.api.inject.ApplicationLifecycle
import slick.basic.DatabasePublisher
import slick.jdbc.GetResult

import scala.concurrent.Future



@Singleton
class PostgresqlRepository @Inject() (applicationLifecycle: ApplicationLifecycle) extends Repository {

  import AppExecutionContexts.streamContext
  import GeometryReaders._
  import utilities.Utils._

  lazy val database = Database.forConfig("fs.postgresql")

  applicationLifecycle.addStopHook(
      () => Utils.withInfo("Closing database") { Future.successful(database.close) }
  )

  case class Row(id: String, geometry: String, json: JsObject)

  implicit val getRowResult = GetResult(r => Row(r.nextString, r.nextString(), Json.parse(r.nextString()).asInstanceOf[JsObject]))
  implicit val getMetadataResult = GetResult(r => {
    val jsEnv = json(r.nextString())
    val env = Json.fromJson[Envelope](jsEnv) match {
      case JsSuccess(value, _) => value
      case _                   => throw new RuntimeException("Invalid envelope JSON format.")
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
    database.run(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException( dbe ) => throw dbe
      }
  }

  override def listDatabases: Future[List[String]] =
    database
      .run(Sql.LIST_SCHEMA)
      .map { _.toList }
      .recover {
        case MappableException( dbe ) => throw dbe
      }


  override def dropDb(dbname: String): Future[Boolean] = {
    database
      .run(Sql.DROP_SCHEMA(dbname).transactionally)
      .map(_ => true)
      .recover {
        case MappableException( dbe ) => throw dbe
      }
  }

  override def createCollection(dbName: String, colName: String, md: Metadata): Future[Boolean] = {
    val dbio = Sql.CREATE_COLLECTION_TABLE(dbName, colName) andThen
      Sql.INSERT_METADATA_JSON_COLLECTION(dbName, colName, md) andThen
      Sql.CREATE_COLLECTION_SPATIAL_INDEX(dbName, colName) andThen
      Sql.CREATE_COLLECTION_ID_INDEX(dbName, colName, md.idType)

    database.run(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException( dbe ) => throw dbe
      }
  }

  override def registerCollection(db: String, collection: String, spec: Metadata): Future[Boolean] =
    withInfo(s"Starting with Registration of $db / $collection ") {

      def reg(db: String, meta: Metadata): Future[Boolean] = database.run(
        Sql.INSERT_METADATA_REGISTERED(db, collection, meta)
      ).map(_ => true)

      for {
        exists <- existsCollection(db, collection) flatMap { b =>
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
    database
      .run(Sql.SELECT_COLLECTION_NAMES(dbname))
      .map(_.toList)
      .recover {
        case MappableException( dbe ) => throw dbe
      }

  /**
    * Retrieves the collection metadata from the server, but does not count number of rows
    *
    * @param db         the database
    * @param collection the collection (in fact table)
    * @return metadata, but row count is set to 0
    */
  def metadataFromDb(db: String, collection: String): Future[Metadata] =
  database.run(Sql.SELECT_METADATA(db, collection)) map {
    case Some(v) => v
    case None    => throw CollectionNotFoundException(s"Collection $db/$collection not found.")
  } recover {
    case MappableException( dbe ) => throw dbe
  }


  override def metadata(database: String, collection: String): Future[Metadata] =
    count(database, collection)
      .flatMap { cnt => metadataFromDb(database, collection).map(md => md.copy(count = cnt))
      }

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] = {
    val dbio = Sql.DELETE_METADATA(dbName, colName) andThen
      Sql.DELETE_VIEWS_FOR_TABLE(dbName, colName) andThen
      Sql.DROP_TABLE(dbName, colName)
    database.run(dbio.transactionally)
      .map(_ => true)
      .recover {
        case MappableException( dbe ) => throw dbe
      }
  }


  override def count(db: String, collection: String): Future[Long] =
    database
      .run(Sql.SELECT_COUNT(db, collection))
      .recover {
        case MappableException( dbe ) => throw dbe
      }


  override def existsCollection(dbName: String, colName: String): Future[Boolean] =
    database.run(Sql.SELECT_COLLECTION_NAMES(dbName))
      .map(_.exists(_ == colName))
      .recover {
        case MappableException( dbe ) => throw dbe
      }


  override def writer(db: String, collection: String): FeatureWriter = new PGWriter(this, db, collection)

//  private def selectEnumerator(md: Metadata): QueryResultEnumerator =
//    if (md.jsonTable) JsonQueryResultEnumerator
//    else new TableQueryResultEnumerator(md)

  override def query(db: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
                     limit: Option[Int] = None): Future[CountedQueryResult] = {

    val projectingReads: Option[Reads[JsObject]] =
      (toJsPathList _ andThen JsonHelper.mkProjection) (spatialQuery.projection)

    //get the enumerator
    //TODO -- the role of the QueryResultEnumerator is now taken up by the GetRow implicits!!
    //val enumerator = selectEnumerator(spatialQuery.metadata)

    //get the count
    val stmtTotal = Sql.SELECT_TOTAL_IN_QUERY(db, collection, spatialQuery)
    val fCnt: Future[Option[Long]] = database.run(stmtTotal).map(Some(_))

    //get the data
    def project(js: JsObject): Option[JsObject] = projectingReads match {
      case None              => Some(js)
      case Some(projections) => js.asOpt(projections)
    }

    val dataStmt = Sql.SELECT_DATA(db, collection, spatialQuery, start, limit)

    val disableAutocommit = SimpleDBIO(_.connection.setAutoCommit(false))

    val publisher: DatabasePublisher[JsObject] = database.stream(disableAutocommit andThen dataStmt.withStatementParameters(fetchSize = 128))
      .mapResult { case Row( _, _, json) => project(json).get
      }

    for {
      cnt <- fCnt
    } yield
      (cnt, Streams.publisherToEnumerator(publisher))

  }


  override def delete(db: String, collection: String, query: BooleanExpr): Future[Boolean] =
    database.run(Sql.DELETE_DATA(db, collection, PGJsonQueryRenderer.render(query))) map { _ => true }

  def batchInsert(db: String, collection: String, jsons: Seq[(JsObject, Polygon)]): Future[Long] = {
    def id(json: JsValue): Any = json match {
      case JsString(v) => v
      case JsNumber(i) => i
      case _           => throw new IllegalArgumentException("No ID property of type String or Number")
    }
    val paramValues = jsons.map {
      case (json, env) => (id((json \ "id").getOrElse(JsNull)), unescapeJson(json), org.geolatte.geom.codec.Wkb.toWkb(env).toString)
    }
    val dbio = DBIO.sequence(paramValues.map { case (id, json, geom) => Sql.INSERT_DATA(db,collection, id.toString, json, geom) })
    database.run(dbio).map ( _.sum )
  }

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] =
    metadataFromDb(database, collection)
      .map { md =>
        (FeatureTransformers.envelopeTransformer(md.envelope), FeatureTransformers.validator(md.idType))
      }.flatMap { case (evr, validator) =>
      batchInsert(database, collection, Seq((json.as(validator), json.as[Polygon](evr)))).map(_ => true)
    }.recover {
      case t: play.api.libs.json.JsResultException =>
        throw new InvalidParamsException("Invalid Json object")
    }

  def update(db: String, collection: String, query: BooleanExpr, newValue: JsObject, envelope: Polygon): Future[Int] = {
    val whereExpr = PGJsonQueryRenderer.render(query)
    val stmt = Sql.UPDATE_DATA(db, collection, whereExpr, Json.stringify(newValue),Wkb.toWkb(envelope).toString)
    database.run(stmt)
  }

  override def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int] =
    metadataFromDb(database, collection)
      .map { md => FeatureTransformers.envelopeTransformer(md.envelope)
      }.flatMap { implicit evr => {
      val ne = updateSpec.as[Polygon] //extract new envelope
      update(database, collection, query, updateSpec, ne)
    }
    }

  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] = {


    val idq = (json \ "id").getOrElse(JsNull) match {
      case JsNumber(i) => s" id = $i"
      case JsString(i) => s" id = '$i'"
      case _           => throw new IllegalArgumentException("Id neither string nor number in json.")
    }

    val expr = QueryParser.parse(idq).get

    //TOODO -- simplify this by doing upsert in SQL like here http://www.the-art-of-web.com/sql/upsert/
    // to be later replaced by postgresql 9.5 upsert behavior
    metadataFromDb(database, collection).map { md =>
      new SpatialQuery(windowOpt = None, intersectionGeometryWktOpt = None, queryOpt = Some(expr), metadata = md)
    }.flatMap { q =>
      query(database, collection, q)
    }.flatMap { case (_, e) =>
      e(Iteratee.head[JsObject])
    }.flatMap { i =>
      i.run
    }.flatMap {
      case Some(v) => update(database, collection, expr, json).map(_ => true)
      case _       => insert(database, collection, json)
    }
  }

  /**
    * Saves a view for the specified database and collection.
    *
    * @param db   the database for the view
    * @param collection the collection for the view
    * @param viewDef    the view definition
    * @return eventually true if this save resulted in the update of an existing view, false otherwise
    */
  override def saveView(db: String, collection: String, viewDef: JsObject): Future[Boolean] = {
    val viewName = (viewDef \ "name").as[String]

    getViewOpt(db, collection, viewName).flatMap {
      case Some(_) => dropView(db, collection, viewName)
      case _       => Future.successful(false)
    } flatMap { isOverWrite =>
      database.run(Sql.INSERT_VIEW(db, collection, viewName, viewDef)) map {
        _ => isOverWrite
      }
    }
  }

  override def dropView(db: String, collection: String, id: String): Future[Boolean] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) database.run(Sql.DELETE_VIEW(db, collection, id)) map { _ => true }
      else throw new CollectionNotFoundException()
    }

  override def getView(database: String, collection: String, id: String): Future[JsObject] =
    getViewOpt(database, collection, id).map { opt =>
      if (opt.isDefined) opt.get
      else throw new ViewObjectNotFoundException()
    }

  def getViewOpt(db: String, collection: String, id: String): Future[Option[JsObject]] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) {
        database.run(Sql.GET_VIEW(db, collection, id)) map {
          case Some(view) => view.asOpt[JsObject](Formats.ViewDefOut(db, collection))
          case _ => None
        }
      }
      else throw new CollectionNotFoundException()
    }

  override def getViews(db: String, collection: String): Future[List[JsObject]] =
    existsCollection(db, collection).flatMap { exists =>
      if (exists) database.run(Sql.GET_VIEWS(db, collection)) map { _.toList }
      else throw new CollectionNotFoundException()
    }

  override def createIndex(dbName: String, colName: String, indexDef: IndexDef): Future[Boolean] = {
    val fRes = if (indexDef.regex) database.run(
      Sql.CREATE_INDEX_WITH_TRGM(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
    )
    else database.run (
      Sql.CREATE_INDEX(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
    )

    fRes map { _ => true } recover {
      case MappableException( dbe ) => throw dbe
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
      case _               => "text"
    }

    val path = (for (m <- pathElRegex.findAllMatchIn(defText)) yield m group 1) mkString "."

    if (path.isEmpty) None //empty path, means not an index on JSON value (maybe spatial, maybe on ID
    else Some(IndexDef(name, path, cast, isForRegex)) //we can't determine the cast used during definition of index
  }

  private def getInternalIndices(dbName: String, colName: String): Future[List[IndexDef]] =
    existsCollection(dbName, colName).flatMap { exists =>
      if (exists) database.run(Sql.SELECT_INDEXES_FOR_TABLE(dbName, colName)) map {
        seq => seq.map(rd => toIndexDef(rd._1, rd._2)).collect {
          case Some(d) => d
        }.toList
      }
      else throw new CollectionNotFoundException()
    }

  override def getIndices(dbName: String, colName: String): Future[List[String]] =
    getInternalIndices(dbName, colName).map(listIdx => listIdx.filter(q => q.path != "id").map(_.name))

  override def getIndex(dbName: String, colName: String, indexName: String): Future[IndexDef] =
    getInternalIndices(dbName, colName).map { listIdx =>
      listIdx.find(q => q.name == indexName) match {
        case Some(idf) => idf
        case None      => throw new IndexNotFoundException(s"Index $indexName on $dbName/$colName not found.")
      }
    }

  override def dropIndex(db: String, collection: String, index: String): Future[Boolean] =
    database.run(Sql.DROP_INDEX(db, collection, index)) map { _ => true }


  //************************************************************************
  //Private Utility methods
  //************************************************************************

  //TODO -- fix that database.runs are "guarded"
  private def guardReady[T](block: => T): T = this.migrationsStatus match {
    case Some(b) => block
    //    case Some(false) => throw NotReadyException("Migrations failed, check the logs")
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

    database.run(Sql.GET_TABLE_PRIMARY_KEY(db, coll)) map {
      toPKeyData(_)
    } map {
      case Some(pk) => pk
      case None     => throw new InvalidPrimaryKeyException(s"Can't determine pkey configuration")
    }
  }

  private def toJsPathList(flds: List[String]): List[JsPath] = {

    val paths = flds.map {
      spath => spath.split("\\.").foldLeft[JsPath](__)((jsp, pe) => jsp \ pe)
    }

    if (paths.isEmpty) paths
    else paths ++ List(__ \ "type", __ \ "geometry", __ \ "id")
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

  private def toColName(str: String): String = str.replaceAll("-", "_")

  private def toGeometry(str: String): Geometry = Wkb.fromWkb(ByteBuffer.from(str))

  //  private def rowData2Properties(rd: RowData, columnNames: Seq[String]) : JsObject = {
  //    val props : Seq[(String, JsValueWrapper)] = for {
  //      c <- columnNames if c != "geometry" && c != "id"
  //      vs <- rd(c)
  //    } yield (c, vs)
  //    Json.obj("properties" -> Json.obj(props:_*))
  //  }


  //These are the SQL statements for managing and retrieving data
  object Sql {

    import MetadataIdentifiers._

    val LIST_SCHEMA = sql"select schema_name from information_schema.schemata".as[String]

    private def fldSortSpecToSortExpr(spec: FldSortSpec): String = {
      //we use the #>> operator for 9.3 support, which extracts to text
      // if we move to 9.4  or later with jsonb then we can use the #> operator because jsonb are ordered
      s" json #>> '{${spec.fld.split("\\.") mkString ","}}' ${spec.direction.toString}"
    }

    def sort(query: SpatialQuery): String = query.metadata.jsonTable match {
      case true =>
        if (query.sort.isEmpty) "ID"
        else query.sort.map { arr => fldSortSpecToSortExpr(arr) } mkString ","
      case _    =>
        def colName(s: String): String = if (s.trim.startsWith("properties.")) s.trim.substring(11) else s.trim
        if (query.sort.isEmpty) query.metadata.pkey else query.sort.map(f => s"${colName(f.fld)} ${f.direction} ") mkString ","
    }


    def condition(query: SpatialQuery): Option[String] = {

      val renderer = if (query.metadata.jsonTable) PGJsonQueryRenderer else PGRegularQueryRenderer
      val geomCol = if (query.metadata.jsonTable) "geometry" else query.metadata.geometryColumn

      def bboxIntersectionRenderer(wkt: String) = s"$geomCol && ${single_quote(wkt)}::geometry"
      def geomIntersectionRenderer(wkt: String) = s"ST_Intersects($geomCol, ${single_quote(wkt)}::geometry)"

      val windowOpt = query.windowOpt.map(env => Wkt.toWkt(FeatureTransformers.toPolygon(env))).map(bboxIntersectionRenderer)
      val intersectionOpt = query.intersectionGeometryWktOpt.map(geomIntersectionRenderer)
      val attOpt = query.queryOpt.map(renderer.render)

      (windowOpt ++ intersectionOpt ++ attOpt).reduceOption((condition1, condition2) => s"$condition1 and $condition2")
    }

    def SELECT_TOTAL_IN_QUERY(db: String, col: String, query: SpatialQuery): DBIO[Long] =
      sql"""
         SELECT COUNT(*)
         FROM #${quote(db)}.#${quote(col)}
         #${condition(query).map(c => s"WHERE $c").getOrElse("")}
     """.as[Long].map(_.head)


    def SELECT_DATA(db: String, col: String, query: SpatialQuery, start: Option[Int] = None, limit: Option[Int] = None) = {

      val cond = condition(query)

      val limitClause = limit match {
        case Some(lim) => s"\nLIMIT $lim"
        case _         => ""
      }

      val offsetClause = start match {
        case Some(s) => s"\nOFFSET $s"
        case _       => ""
      }

      val projection =
        if (query.metadata.jsonTable) "ID, geometry, json"
        else s" ST_AsGeoJson( ${query.metadata.geometryColumn}, 15, 3 ) as __geojson, * "

      sql"""
         SELECT #$projection
         FROM #${quote(db)}.#${quote(col)}
         #${cond.map(c => s"WHERE $c").getOrElse("")}
         ORDER BY #${sort(query)}
         #$offsetClause
         #$limitClause
     """.as[Row]

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
     """.as[JsObject].map{_.headOption}

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
     """.as[(String,String)]

    def DROP_INDEX(db: String, col: String, indexName: String) =
      sqlu"""
         DROP INDEX IF EXISTS #${quote(db)}.#${quote(indexName)}
     """

  }


  object MappableException {
    def getStatus(dbe : PSQLException) = dbe.getServerErrorMessage.getSQLState
    def getMessage(dbe: PSQLException) = dbe.getServerErrorMessage.getMessage
    def unapply(t : Throwable): Option[RuntimeException] =  t match {
      case t: PSQLException if getStatus( t ).contains( "42P06" ) =>
        Some(DatabaseAlreadyExistsException(getMessage(t)))
      case t: PSQLException if getStatus( t ).contains( "42P07" ) =>
        Some(CollectionAlreadyExistsException(getMessage(t)))
      case t: PSQLException if getStatus( t ).contains( "3F000" ) =>
        Some(DatabaseNotFoundException(getMessage(t)))
      case t: PSQLException if getStatus( t ).contains( "42P01" ) =>
        Some(CollectionNotFoundException(getMessage(t)))
      case _ => None
    }
  }

}
