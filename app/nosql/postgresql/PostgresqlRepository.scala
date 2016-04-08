package nosql.postgresql

import com.github.mauricio.async.db.{Connection, QueryResult, RowData}
import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import config.{AppExecutionContexts, ConfigurationValues}
import controllers.{Formats, IndexDef}
import nosql._
import nosql.json.GeometryReaders
import org.geolatte.geom.codec.{Wkb, Wkt}
import org.geolatte.geom.{ByteBuffer, Envelope, Geometry, Polygon}
import play.api.Logger
import play.api.libs.iteratee.{Enumerator, Iteratee}
import querylang.{BooleanExpr, QueryParser}
import utilities.JsonHelper

import scala.concurrent.Future

/**
 * A NoSQL feature store repository for Postgresql/Postgis
 *
 * Created by Karel Maesen, Geovise BVBA on 11/12/14.
 */
object PostgresqlRepository extends Repository {

  import AppExecutionContexts.streamContext
  import play.api.libs.json._
  import GeometryReaders._

  lazy val url = {
    val parsed = URLParser.parse(ConfigurationValues.PgConnectionString)
    Logger.info("Connecting to postgresql database with URL : " + parsed)
    parsed
  }

  lazy val database = url.database.getOrElse(url.username)
  lazy val factory = new PostgreSQLConnectionFactory( url )

  val poolConfig = PoolConfiguration(
        maxObjects = ConfigurationValues.PgMaxObjects,
        maxIdle = ConfigurationValues.PgMaxIdle,
        maxQueueSize = ConfigurationValues.PgMaxQueueSize,
        validationInterval = ConfigurationValues.PgMaxValidationinterval)

  Logger.info(s"Postgresql Repo initialized with ConnectionPool: $poolConfig")

  lazy val pool = new ConnectionPool(factory, poolConfig)

  var migrationsStatus: Option[Boolean] = None

  {
    import Utils._
    val result : Future[Boolean] = listDatabases.flatMap { dbs => sequence(dbs)( s => Migrations.executeOn(s) )}
    result.onSuccess {
      case b => migrationsStatus = Some(b)
    }
  }

  override def createDb(dbname: String): Future[Boolean] = executeStmtsInTransaction(
    s"${Sql.CREATE_SCHEMA(dbname)}; ${Sql.CREATE_METADATA_TABLE_IN(dbname)}; ${Sql.CREATE_VIEW_TABLE_IN(dbname)}"
  ).map( _ => true)

  override def listDatabases: Future[List[String]] = executeStmt(Sql.LIST_SCHEMA){
    qr => qr.rows.map( rs => rs.foldLeft(List[String]())( (ac, data) => data(0).asInstanceOf[String]::ac))
        .getOrElse(List[String]())
  }


  override def dropDb(dbname: String): Future[Boolean] =
    executeStmtsInTransaction(Sql.DROP_SCHEMA(dbname)){ _ => true }.map(_.head)


  override def createCollection(dbName: String, colName: String, md: Metadata): Future[Boolean] = {
    val stmts = List(
      Sql.CREATE_COLLECTION_TABLE(dbName, colName),
      Sql.INSERT_METADATA(dbName, colName, md),
      Sql.CREATE_COLLECTION_SPATIAL_INDEX(dbName, colName),
      Sql.CREATE_COLLECTION_ID_INDEX(dbName, colName, md.idType)
    )
    executeStmtsInTransaction( stmts :_*) map ( _ => true )
  }

  override def registerCollection(db: String, collection: String): Future[Boolean] = ???

  override def listCollections(dbname: String): Future[List[String]] =
    executeStmt(Sql.SELECT_COLLECTION_NAMES(dbname)){
      toList(_)(row => Some(row(0).asInstanceOf[String]))
    }

  /**
   * Retrieves the collection metadata from the server, but does not count number of rows
    *
    * @param database the database (schema)
   * @param collection the collection (table)
   * @return metadata, but row count is set to 0
   */
  def metadataFromDb(database: String, collection: String) : Future[Metadata] = {

    def mkMetadata(row: RowData) : Metadata = {
      val jsEnv = Json.parse(row(0).asInstanceOf[String])
      val env = Json.fromJson[Envelope](jsEnv) match {
        case JsSuccess(value, _) => value
        case _ => throw new RuntimeException("Invalid envellopre JSON format.")
      }
      Metadata(collection, env, row(1).asInstanceOf[Int], row(2).asInstanceOf[String])
    }

    def queryResult2Metadata(qr: QueryResult) =
      qr.rows match {
        case Some(rs) if rs.nonEmpty => mkMetadata(rs.head)
        case _ => throw new CollectionNotFoundException()
      }
    executeStmt(Sql.SELECT_METADATA(database, collection))( queryResult2Metadata )
  }


  override def metadata(database: String, collection: String): Future[Metadata] =
     count(database, collection)
      .flatMap { cnt => metadataFromDb(database, collection).map( md => md.copy(count = cnt) )
    }

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] =
  executeStmtsInTransaction(
    Sql.DELETE_METADATA(dbName, colName),
    Sql.DELETE_VIEWS_FOR_TABLE(dbName, colName),
    Sql.DROP_TABLE(dbName, colName)
  ) map ( _ => true )


  override def count(database: String, collection: String): Future[Long] =
    executeStmt(Sql.SELECT_COUNT(database, collection)) {
      first(row => Some(row(0).asInstanceOf[Long]))(_).get
    }

  override def existsCollection(dbName: String, colName: String): Future[Boolean] =
    executeStmt(Sql.SELECT_COLLECTION_NAMES(dbName)) { qr =>
      qr.rows match {
        case Some(rs) =>
          rs.exists( row => row(0).asInstanceOf[String].equalsIgnoreCase(colName))
        case _ => throw new RuntimeException("Query failed to return a row set")
      }
    }

  override def writer(database: String, collection: String): FeatureWriter = new PGWriter(database, collection)

  override def query(database: String, collection: String, spatialQuery: SpatialQuery, start : Option[Int] = None,
                     limit: Option[Int] = None): Future[CountedQueryResult] = {

    val projectingReads : Option[Reads[JsObject]] =
      (toJsPathList _ andThen JsonHelper.mkProjection )(spatialQuery.projection)

    val sortExpr: String = spatialQuery.sort.map {
      arr => fldSortSpecToSortExpr(arr)
    } mkString ","

    //get the count
    val stmtTotal = Sql.SELECT_TOTAL_IN_QUERY(database, collection, spatialQuery)
    val fCnt = executeStmt(stmtTotal){ first(rd => Some(rd(0).asInstanceOf[Long]))(_).get }

    //get the data
    val dataStmt = Sql.SELECT_DATA(database, collection, spatialQuery, sortExpr, start, limit)
    val fEnum = executeStmt(dataStmt){ enumerate(_,projectingReads) }

    {for{
      cnt <- fCnt
      enum <- fEnum
    } yield
      (Some(cnt),enum)
    }

  }

  //this method is there for testing purposes only
  //TODO -- remove this method
  protected def doSelect(sql: String) : Future[List[RowData]] = {

    executeStmt(sql) { _.rows match {
      case Some(rs) =>
        val buf = new Array[RowData](rs.size)
        rs.copyToArray(buf)
        buf.toList

      case _ => List[RowData]()
    }}
  }

  override def delete(database: String, collection: String, query: BooleanExpr): Future[Boolean] =
    executeStmt(Sql.DELETE_DATA(database, collection, PGQueryRenderer.render(query))){ _ => true}

  def batchInsert(database: String, collection: String, jsons: Seq[(JsObject, Polygon)] ): Future[Long] = {
    def id(json: JsValue) : Any = json match {
      case JsString(v) => v
      case JsNumber(i) => i
      case _ => throw new IllegalArgumentException("No ID property of type String or Number")
    }
    val paramValues : Seq[Seq[Any]] = jsons.map{
      case (json, env) =>  Seq( id(json \ "id") , unescapeJson(json), org.geolatte.geom.codec.Wkb.toWkb(env))
    }
    val numRowsAffected : Future[List[Long]] = executePreparedStmts(Sql.INSERT_DATA(database, collection),
      paramValues){_.rowsAffected}
    numRowsAffected.map( cnts => cnts.foldLeft(0L)( _ + _))
  }

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] =
    metadataFromDb(database, collection)
      .map{ md =>
      (FeatureTransformers.envelopeTransformer(md.envelope), FeatureTransformers.validator(md.idType))
    }.flatMap { case (evr, validator) =>
      batchInsert(database, collection, Seq( (json.as(validator), json.as[Polygon](evr)))) .map( _ => true)
    }.recover {
      case t: play.api.libs.json.JsResultException =>
        throw new InvalidParamsException("Invalid Json object")
    }

  def update(database: String, collection: String, query: BooleanExpr, newValue: JsObject, envelope: Polygon) : Future[Int] = {
    val whereExpr = PGQueryRenderer.render(query)
    val stmt = Sql.UPDATE_DATA(database, collection, whereExpr)
    executePreparedStmt(stmt, Seq(newValue, Wkb.toWkb(envelope))){ _.rowsAffected.toInt }
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


    val idq = json \ "id" match {
      case JsNumber(i) => s" id = $i"
      case JsString(i) => s" id = '$i'"
      case _           => throw new IllegalArgumentException("Id neither string nor number in json.")
     }

    val expr = QueryParser.parse(idq).get
    val q = new SpatialQuery(None, Some(expr))

    query(database, collection,q)
      .flatMap{ case ( _, e)  =>
      e(Iteratee.head[JsObject])
    }.flatMap{ i =>
      i.run
    }.flatMap{
      case Some(v) => update(database, collection, expr, json).map( _ => true)
      case _ => insert(database, collection, json)
    }
  }

  /**
   * Saves a view for the specified database and collection.
   *
   * @param database the database for the view
   * @param collection the collection for the view
   * @param viewDef the view definition
   * @return eventually true if this save resulted in the update of an existing view, false otherwise
   */
  override def saveView(database: String, collection: String, viewDef: JsObject): Future[Boolean] = {
    val viewName = (viewDef \ "name").as[String]

    getViewOpt(database, collection, viewName).flatMap {
      case Some(_) => dropView(database, collection, viewName)
      case _ =>  Future.successful(false)
    } flatMap { isOverWrite =>
       executeStmtsInTransaction(Sql.INSERT_VIEW(database, collection, viewName, viewDef)){
          _ => isOverWrite
        }.map {
         _.head
       }
      }
    }

  override def dropView(database: String, collection: String, id: String): Future[Boolean] =
    existsCollection(database, collection).flatMap { exists =>
      if (exists) executeStmtInTransaction(Sql.DELETE_VIEW(database, collection, id)){ _ => true }
      else throw new CollectionNotFoundException()
      }

  override def getView(database: String, collection: String, id: String): Future[JsObject] =
    getViewOpt(database,collection,id).map { opt =>
      if (opt.isDefined) opt.get
      else throw new ViewObjectNotFoundException()
    }

  def getViewOpt(database: String, collection: String, id: String): Future[Option[JsObject]] =
    existsCollection(database, collection).flatMap { exists =>
      if (exists) {
        executePreparedStmt(Sql.GET_VIEW(database), Seq(collection,id)) {
          first( rd => toJson(rd(0).asInstanceOf[String])(Formats.ViewDefOut(database, collection)))(_)
        }
      }
      else throw new CollectionNotFoundException()
    }

  override def getViews(database: String, collection: String): Future[List[JsObject]] =
    existsCollection(database, collection).flatMap { exists =>
      if (exists) executePreparedStmt(Sql.GET_VIEWS(database), Seq(collection)) { qr => toProjectedJsonList(qr, None) }
      else throw new CollectionNotFoundException()
    }

  override def createIndex(dbName: String, colName: String, indexDef: IndexDef) : Future[Boolean] =
      if ( indexDef.regex ) executeStmtInTransaction(
          Sql.CREATE_INDEX_WITH_TRGM(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
        ) { _ => true}
      else executeStmtInTransaction(
          Sql.CREATE_INDEX(dbName, colName, indexDef.name, indexDef.path, indexDef.cast)
        ){_ => true}

  private def toIndexDef(name: String, defText: String) : Option[IndexDef] = {
    val pathElRegex = "\\'(\\w+)\\'".r
    val methodRegex = "USING (\\w+) ".r
    val castRegex = "::(\\w+)\\)\\)$".r

    val method = methodRegex.findFirstMatchIn(defText).map{ m => m group 1 }
    val isForRegex = method.exists( _ == "gist" )

    val cast = castRegex.findFirstMatchIn(defText).map{ m => m group 1} match {
      case Some("boolean") => "bool"
      case Some("numeric") => "decimal"
      case _ => "text"
    }

    val path = (for ( m <- pathElRegex.findAllMatchIn(defText) ) yield m group 1 ) mkString "."

    if (path.isEmpty) None //empty path, means not an index on JSON value (maybe spatial, maybe on ID
    else Some(IndexDef(name, path, cast, isForRegex)) //we can't determine the cast used during definition of index
  }

  private def getInternalIndices(dbName: String, colName: String) : Future[List[IndexDef]] =
    existsCollection(dbName, colName).flatMap { exists =>
    if(exists) executeStmt( Sql.SELECT_INDEXES_FOR_TABLE(dbName, colName) ){
      toList(_)(rd => toIndexDef(rd(0).asInstanceOf[String], rd(1).asInstanceOf[String]))
    }
    else throw new CollectionNotFoundException()
  }

  override def getIndices(dbName: String, colName: String): Future[List[String]] =
    getInternalIndices(dbName, colName).map( listIdx => listIdx.filter(q => q.path != "id").map(_.name) )

  override def getIndex(dbName: String, colName: String, indexName: String): Future[IndexDef] =
    getInternalIndices(dbName, colName).map{ listIdx =>
    listIdx.find(q => q.name == indexName) match {
        case Some(idf) => idf
        case None => throw new IndexNotFoundException(s"Index $indexName on $dbName/$colName not found.")
      }
    }

  override def dropIndex(database: String, collection: String, index: String): Future[Boolean] =
    executeStmtInTransaction( Sql.DROP_INDEX(database, collection, index) ){ _ => true}


  //************************************************************************
  //Private Utility methods
  //************************************************************************

  private def guardReady[T](block: => T) : T = this.migrationsStatus match {
    case Some(b) => block
//    case Some(false) => throw NotReadyException("Migrations failed, check the logs")
    case _ => throw NotReadyException("Busy migrating databases")
  }

  private def  sendstatement[T](stmt: String)(implicit resultHandler: QueryResult => T, c: Connection) : Future[T] = {
    Logger.debug("SQL IN TRANSACTION: " + stmt)
    c.sendQuery(stmt).map(qr => resultHandler(qr))
  }

  // if resultHandler is not specified, the identity function is used by implicits of Predef
  private def executeStmtsInTransaction[T](sql: String*)(implicit resultHandler: QueryResult => T) : Future[List[T]] =
    doInTransaction { connection =>
      sql.foldLeft(Future.successful(List[T]())) {
        (futurelist: Future[List[T]], stmt: String) => futurelist.flatMap[List[T]]{l =>
            sendstatement(stmt)(resultHandler, connection).map(t => t :: l)
        }.map(l => l.reverse)
      }
    }

  private def executeStmtInTransaction[T](sql: String)(implicit resultHandler: QueryResult =>T) : Future[T] =
      doInTransaction { connection =>
          sendstatement(sql)(resultHandler, connection)
      }

  private def sendprepared[T](sql: String, vals: Seq[Any])(implicit c : Connection) : Future[QueryResult] = {
    Logger.debug( s"\t\t Executing Prepared statement $sql in transactions with values ${vals mkString ", "}" )
    c.sendPreparedStatement(sql, vals)
  }

  private def executePreparedStmts[T](sql: String, values: Seq[Seq[Any]])(implicit resultHandler: QueryResult => T) : Future[List[T]] =
    doInTransaction { implicit c =>
      values.foldLeft(Future.successful(List[T]())) {
        (flist: Future[List[T]], vals: Seq[Any]) =>
          flist.flatMap { l =>
            sendprepared(sql, vals)
              .map(resultHandler)
              .map(t => t :: l)
          }.map(l => l.reverse)
      }
    }


  private def executePreparedStmt[T](sql: String, values: Seq[Any])(implicit resultHandler: QueryResult => T) : Future[T] =
    doInTransaction { implicit c =>
      sendprepared(sql, values).map( resultHandler)
    }

  private def doInTransaction[T](block : Connection => Future[T]) : Future[T] = guardReady{
    pool.inTransaction { implicit c =>
      block(c)
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }
  }

  def executeStmt[T](sql: String)(resultHandler: QueryResult => T): Future[T] = guardReady {
    Logger.debug("SQL : " + sql)
    pool.sendQuery(sql)
      .map {
      resultHandler
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }
  }

  private def toJson(text : String)(implicit reads : Reads[JsObject]) : Option[JsObject] =
    Json
      .parse(text)
      .asOpt[JsObject](reads)

  private def toJsPathList(flds: List[String]) : List[JsPath] = {

    val paths = flds.map {
      spath => spath.split("\\.").foldLeft[JsPath](__)((jsp, pe) => jsp \ pe)
    }

    if (paths.isEmpty) paths
    else paths ++ List( __ \ "type",  __ \ "geometry")
  }

  private def fldSortSpecToSortExpr(spec: FldSortSpec) : String = {
      //we use the #>> operator for 9.3 support, which extracts to text
      // if we move to 9.4  or later with jsonb then we can use the #> operator because jsonb are ordered
      s" json #>> '{${spec.fld.split("\\.") mkString ","}}' ${spec.direction.toString}"
  }

  private def first[T](rowF : RowData => Option[T])(qr: QueryResult) : Option[T] = qr.rows match {
    case Some(rs) if rs.nonEmpty => rowF(rs.head)
    case _ => None  
  }


  private def toList[T](qr: QueryResult)(rowF : RowData => Option[T]): List[T] = qr.rows match {
    case Some(rs) =>
      rs.map(rowF).collect{case Some(v) => v}.toList
    case _ => List[T]()
  }

  private def toProjectedJsonList(qr: QueryResult, optProj : Option[Reads[JsObject]]) : List[JsObject] = {

    def json(row : RowData) : Option[JsObject] = toJson(row(0).asInstanceOf[String])

    def project(reads : Reads[JsObject])(jsOpt : Option[JsObject]) : Option[JsObject] = jsOpt flatMap (_.asOpt(reads))

    val transformFunc = optProj match {
      case Some(reads) => json _ andThen project(reads)
      case _ => json _
    }
    toList(qr)(transformFunc)
  }

  private def enumerate(qr: QueryResult, optProj : Option[Reads[JsObject]]) : Enumerator[JsObject] = {
    Enumerator.enumerate(toProjectedJsonList(qr, optProj))
  }

//
//    SQL Statements and utility functions.
//
//    Note that we cannot use prepared statements for DDL's

  //TODO -- should we check for quotes, spaces etc.  in str?
  // alternative is to use escapeSql in the org.apache.commons.lang.StringEscapeUtils
  private def quote(str: String) : String = "\"" + str + "\""
  private def single_quote(str: String) : String = "'" + str + "'"
  private def unescapeJson(js: JsObject):String = Json.stringify(js).replaceAll("'", "''")
  private def toColName(str: String) : String = str.replaceAll("-", "_")
  private def toGeometry(str: String) : Geometry = Wkb.fromWkb(ByteBuffer.from(str))

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

    val LIST_SCHEMA = "select schema_name from information_schema.schemata"

    def condition(query : SpatialQuery) : String = {
      val windowOpt = query.windowOpt.map( env => Wkt.toWkt(FeatureTransformers.toPolygon(env)))
      val attOpt = query.queryOpt.map( PGQueryRenderer.render )
      (windowOpt, attOpt) match {
        case (Some(w),Some(q)) => s"geometry && ${single_quote(w)}::geometry and $q"
        case (Some(w), _ ) => s"geometry && ${single_quote(w)}::geometry"
        case (_, Some(q)) => q
        case _ => "true"
      }
    }

    def SELECT_TOTAL_IN_QUERY(db: String, col: String, query: SpatialQuery) : String =
      s"""
       |SELECT COUNT(*)
       |FROM ${quote(db)}.${quote(col)}
       |where ${condition(query)}
     """.stripMargin



    def SELECT_DATA(db: String, col: String, query: SpatialQuery, sortExpr: String, start: Option[Int] = None, limit: Option[Int] = None): String = {

      val cond = condition(query)

      val limitClause = limit match {
        case Some(lim) => s"\nLIMIT $lim"
        case _ => s"\nLIMIT ${ConfigurationValues.MaxReturnItems}"
      }

      val offsetClause = start match {
        case Some(s) => s"\nOFFSET $s"
        case _ => ""
      }

      s"""
       |SELECT json, ID, geometry
       |FROM ${quote(db)}.${quote(col)}
       |WHERE $cond
       |ORDER BY ${ if (sortExpr.isEmpty) "ID" else sortExpr }
     """.stripMargin + offsetClause + limitClause

    }


    def UPDATE_DATA(db: String, col: String, where: String) : String = {
      s"""UPDATE ${quote(db)}.${quote(col)}
         |SET json = ?, geometry = ?
         |WHERE $where
       """.stripMargin
    }

    def CREATE_SCHEMA(dbname: String) = s"create schema ${quote(dbname)}"

    def DROP_SCHEMA(dbname : String) : String = s"drop schema ${quote(dbname)} CASCADE"

    def CREATE_METADATA_TABLE_IN(dbname : String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(MetadataCollection)} (
          | ${toColName(ExtentField)} JSON,
          | ${toColName(IndexLevelField)} INT,
          | ${toColName(IdTypeField)} VARCHAR(120),
          | ${toColName(CollectionField)} VARCHAR(255) PRIMARY KEY
          | )
       """.stripMargin

    def CREATE_VIEW_TABLE_IN(dbname: String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(ViewCollection)} (
          | COLLECTION VARCHAR,
          | VIEW_NAME VARCHAR,
          | VIEW_DEF JSON,
          | UNIQUE (COLLECTION, VIEW_NAME)
          | )
       """.stripMargin


    def CREATE_COLLECTION_TABLE(dbname : String, tableName : String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(tableName)} (
          | id VARCHAR(255) PRIMARY KEY,
          | geometry GEOMETRY,
          | json JSON
          | )
       """.stripMargin

    def CREATE_COLLECTION_ID_INDEX(dbname: String, tableName: String, idType: String) =
    s"""CREATE INDEX ${quote("idx_"+tableName+"_id")}
       |ON ${quote(dbname)}.${quote(tableName)} ( (json_extract_path_text(json, 'id')::$idType) )
     """.stripMargin

    def CREATE_COLLECTION_SPATIAL_INDEX(dbname: String, tableName: String) =
      s"""CREATE INDEX ${quote(tableName + "_spatial_index")}
      | ON ${quote(dbname)}.${quote(tableName)} USING GIST ( geometry )
     """.stripMargin

    def INSERT_DATA(dbname: String, tableName: String) =
      s"""INSERT INTO ${quote(dbname)}.${quote(tableName)}  (id, json, geometry)
         |VALUES (?, ?, ?)
       """.stripMargin

    def DELETE_DATA(dbname: String, tableName: String, where: String) =
      s"""DELETE FROM ${quote(dbname)}.${quote(tableName)}
        WHERE $where
     """.stripMargin


    def LIST_TABLE_NAMES(dbname: String) = {
      s""" select table_name from information_schema.tables
         | where
         | table_schema = ${single_quote(dbname)}
         | and table_type = 'BASE TABLE'
         | and table_name != ${quote(MetadataCollection)}
       """.stripMargin
    }

    def INSERT_METADATA(dbname: String, tableName: String, md: Metadata) =
      s"""insert into ${quote(dbname)}.${quote(MetadataCollection)} values(
       | ${single_quote(Json.stringify(Json.toJson(md.envelope)))}::json,
       | ${md.level},
       | ${single_quote(md.idType)},
       | ${single_quote(tableName)}
       | )
     """.stripMargin

    def SELECT_COLLECTION_NAMES(dbname: String) =
      s"""select $CollectionField
        from  ${quote(dbname)}.${quote(MetadataCollection)}
     """.stripMargin

    def SELECT_COUNT(dbname: String, tablename: String) =
      s"select count(*) from ${quote(dbname)}.${quote(tablename)}"

    def SELECT_METADATA(dbname: String, tablename: String) =
      s"""select *
        |from ${quote(dbname)}.${quote(MetadataCollection)}
        |where $CollectionField = ${single_quote(tablename)}
      """.stripMargin

    def DELETE_METADATA(dbname: String, tablename: String) =
      s"""delete
       |from ${quote(dbname)}.${quote(MetadataCollection)}
       |where $CollectionField = ${single_quote(tablename)}
     """.stripMargin

    def DROP_TABLE(dbname: String, tablename: String) =
      s""" drop table ${quote(dbname)}.${quote(tablename)}
     """.stripMargin

    def DELETE_VIEWS_FOR_TABLE(dbname: String, tablename: String) =
      s"""
       |DELETE FROM ${quote(dbname)}.${quote(ViewCollection)}
       |WHERE COLLECTION = ${single_quote(tablename)}
     """.stripMargin

    def INSERT_VIEW(dbname: String, tableName: String, viewName: String, json: JsObject) =
      s"""
       |INSERT INTO ${quote(dbname)}.${quote(ViewCollection)} VALUES (
       |  ${single_quote(tableName)},
       |  ${single_quote(viewName)},
       |  ${single_quote(unescapeJson(json))}
       |)
     """.stripMargin

    def DELETE_VIEW(dbname: String, tableName: String, viewName: String) =
      s"""
       |DELETE FROM ${quote(dbname)}.${quote(ViewCollection)}
       |WHERE COLLECTION = ${single_quote(tableName)} and VIEW_NAME = ${single_quote(viewName)}
     """.stripMargin

    def GET_VIEW(dbname: String) =
      s"""
       |SELECT VIEW_DEF
       |FROM ${quote(dbname)}.${quote(ViewCollection)}
       |WHERE COLLECTION = ? AND VIEW_NAME = ?
     """.stripMargin

    def GET_VIEWS(dbname: String) =
      s"""
       |SELECT VIEW_DEF
       |FROM ${quote(dbname)}.${quote(ViewCollection)}
       |WHERE COLLECTION = ?
     """.stripMargin


//    def CHECK_TRGM_EXTENSION() =
//    s"""
//       |select true from pg_extension where extname = 'pg_trgm'
//     """.stripMargin

    def CREATE_INDEX(dbName: String, colName: String, indexName: String, path: String, cast: String)   = {
      val pathExp = path.split("\\.").map(
       el => single_quote(el)
      ).mkString(",")
      s"""CREATE INDEX ${quote(indexName)}
          |ON ${quote(dbName)}.${quote(colName)} ( (json_extract_path_text(json, $pathExp)::$cast) )
      """.stripMargin
    }

    def CREATE_INDEX_WITH_TRGM(dbName: String, colName: String, indexName: String, path: String, cast: String)   = {
      val pathExp = path.split("\\.").map(
        el => single_quote(el)
      ).mkString(",")
      s"""CREATE INDEX ${quote(indexName)}
          |ON ${quote(dbName)}.${quote(colName)} using gist
          |( (json_extract_path_text(json, $pathExp)::$cast) gist_trgm_ops)
      """.stripMargin
    }

    def SELECT_INDEXES_FOR_TABLE(db: String, col: String): String =
      s"""
         |SELECT INDEXNAME, INDEXDEF
         |FROM pg_indexes
         |WHERE schemaname = ${single_quote(db)} AND tablename = ${single_quote(col)}
     """.stripMargin

    def DROP_INDEX(db: String, col: String, indexName: String): String =
    s"""
       |DROP INDEX IF EXISTS ${quote(db)}.${quote(indexName)}
     """.stripMargin

  }


  /**
   * And Extractor that translates a GenericDataseException into the appropratie database Exception
   */
  object MappableException {
    def getStatus(dbe : GenericDatabaseException) = dbe.errorMessage.fields.get('C')
    def getMessage(dbe: GenericDatabaseException) = dbe.errorMessage.fields.getOrElse('M', "Database didn't return a message")
    def unapply(t : Throwable): Option[RuntimeException] =  t match {
      case t: GenericDatabaseException if getStatus(t) == Some("42P06") =>
        Some(new DatabaseAlreadyExistsException(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("42P07") =>
        Some(new CollectionAlreadyExistsException(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("3F000") =>
        Some(new  DatabaseNotFoundException(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("42P01") =>
        Some(new CollectionNotFoundException(getMessage(t)))
      case _ => None
    }
  }


}
