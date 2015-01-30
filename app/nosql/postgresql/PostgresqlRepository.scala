package nosql.postgresql

import com.github.mauricio.async.db.{QueryResult, Connection, ResultSet, RowData}
import com.github.mauricio.async.db.pool.{PoolConfiguration, ConnectionPool}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import config.{AppExecutionContexts, ConfigurationValues}
import controllers.Formats
import nosql._
import nosql.json.GeometryReaders
import org.geolatte.geom.codec.{Wkt, Wkb}
import org.geolatte.geom.{Polygon, Envelope}
import play.api.Logger
import play.api.libs.iteratee.{Iteratee, Enumerator}
import querylang.{QueryParser, BooleanExpr}
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

  //TODO make connection pool configurable
  lazy val pool = new ConnectionPool(factory, PoolConfiguration.Default)

  override def createDb(dbname: String): Future[Boolean] = executeStmtsInTransaction(
    s"${Sql.CREATE_SCHEMA(dbname)}; ${Sql.CREATE_METADATA_TABLE_IN(dbname)}; ${Sql.CREATE_VIEW_TABLE_IN(dbname)}"
  ).map( _ => true)

  override def listDatabases: Future[List[String]] = executeStmt(Sql.LIST_SCHEMA){
    qr => qr.rows.map( rs => rs.foldLeft(List[String]())( (ac, data) => data(0).asInstanceOf[String]::ac))
        .getOrElse(List[String]())
  }


  override def dropDb(dbname: String): Future[Boolean] =
    pool.inTransaction { c =>
      c.sendQuery(Sql.DROP_SCHEMA(dbname))
        .map{
        _ => true
      }.recover {
        case ex: Throwable => {
          Logger.error(s"Problem deleting database $dbname", ex)
          throw new DatabaseDeleteException(s"Unknown exception of type: ${ex.getClass.getCanonicalName} having message: ${ex.getMessage}")
        }
      }
    }

  override def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]): Future[Boolean] = {
    def stmts = spatialSpec match {
      case Some(md) => List( Sql.CREATE_COLLECTION_TABLE(dbName, colName), Sql.INSERT_METADATA(dbName, colName, md)
        , Sql.CREATE_COLLECTION_INDEX(dbName, colName))
      case None =>   List( Sql.CREATE_COLLECTION_TABLE(dbName, colName), Sql.CREATE_COLLECTION_INDEX(dbName, colName))
    }
    executeStmtsInTransaction( stmts :_*) map ( _ => true )
  }

  override def listCollections(dbname: String): Future[List[String]] =
    executeStmt(Sql.SELECT_COLLECTION_NAMES(dbname)){
      toList(_)(row => Some(row(0).asInstanceOf[String]))
    }

  override def metadata(database: String, collection: String): Future[Metadata] = {

    def mkMetadata(row: RowData, cnt: Long) : Metadata = {
      val jsEnv = Json.parse(row(0).asInstanceOf[String])
      val env = Json.fromJson[Envelope](jsEnv) match {
        case JsSuccess(value, _) => value
        case _ => throw new RuntimeException("Invalid envellopre JSON format.")
      }
      Metadata(collection, env, row(1).asInstanceOf[Int], cnt)
    }

    def queryResult2Metadata(qr: QueryResult, cnt: Long) =
      qr.rows match {
        case Some(rs) if rs.size > 0 => mkMetadata(rs.head, cnt)
        case _ => throw new CollectionNotFoundException()
      }

    count(database, collection)
      .flatMap { cnt =>
        executeStmt(Sql.SELECT_METADATA(database, collection))(qr => queryResult2Metadata(qr, cnt))
    }

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
          rs.filter( row => row(0).asInstanceOf[String].equalsIgnoreCase(colName)).nonEmpty
        case _ => throw new RuntimeException("Query failed to return a row set")
      }
    }

  override def writer(database: String, collection: String): FeatureWriter = new PGWriter(database, collection)



  def insert(database: String, collection: String, jsons: Seq[(JsObject,Polygon)] ): Future[Long] = {
    val paramValues : Seq[Seq[Any]] = jsons.map{
        case (json, env) =>  Seq( (json \ "id").toString , unescapeJson(json), org.geolatte.geom.codec.Wkb.toWkb(env))
    }
    val numRowsAffected : Future[List[Long]] = executePreparedStmts(Sql.INSERT_DATA(database, collection), paramValues){_.rowsAffected}
    numRowsAffected.map( cnts => cnts.foldLeft(0L)( _ + _))
  }

  def insert(database: String, collection: String, json: JsObject, env: Polygon ): Future[Boolean] =
    insert(database, collection, Seq((json, env))).map(_ => true)

  override def query(database: String, collection: String, spatialQuery: SpatialQuery, start : Option[Int] = None,
                     limit: Option[Int] = None): Future[CountedQueryResult] = {

    val projectingReads : Option[Reads[JsObject]] =
      spatialQuery.projectionOpt
        .map {
        arr => jsArrayToJsPathList(arr)
      }.map {
        pathList => JsonHelper.mkProjection(pathList)
      }

    //get the count
    val stmtTotal = Sql.SELECT_TOTAL_IN_QUERY(database, collection, spatialQuery)
    val fCnt = executeStmt(stmtTotal){ first(rd => Some(rd(0).asInstanceOf[Long]))(_).get }

    //get the data
    val dataStmt = Sql.SELECT_DATA(database, collection, spatialQuery, start, limit)
    val fEnum = executeStmt(dataStmt){ enumerate(_,projectingReads) }

    {for{
      cnt <- fCnt
      enum <- fEnum
    } yield
      (Some(cnt),enum)
    }

  }

  override def delete(database: String, collection: String, query: BooleanExpr): Future[Boolean] =
    executeStmt(Sql.DELETE_DATA(database, collection, PGQueryRenderer.render(query))){ _ => true}


  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] =
    metadata(database, collection)
      .map{ md =>
      FeatureTransformers.envelopeTransformer(md.envelope)
    }.flatMap { evr =>
      insert(database, collection, json, json.as[Polygon](evr))
    }

  def update(database: String, collection: String, query: BooleanExpr, newValue: JsObject, envelope: Polygon) : Future[Int] = {
    val whereExpr = PGQueryRenderer.render(query)
    val stmt = Sql.UPDATE_DATA(database, collection, whereExpr)
    executePreparedStmt(stmt, Seq(newValue, Wkb.toWkb(envelope))){ _.rowsAffected.toInt }
  }

  override def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int] =
    metadata(database, collection)
      .map { md => FeatureTransformers.envelopeTransformer(md.envelope)
    }.flatMap { implicit evr => {
      val ne = updateSpec.as[Polygon] //extract new envelope
      update(database, collection, query, updateSpec, ne)
    }
    }

  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] = {


    val idq = json \ "id" match {
      case JsNumber(i) => s" id = ${i}"
      case JsString(i) => s" id = '${i}'"
      case _           => throw new IllegalArgumentException("Id neither string nor number in json.")
     }

    val expr = QueryParser.parse(idq).get
    val q = new SpatialQuery(None, Some(expr), None)

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
      if (exists) executeStmtsInTransaction(Sql.DELETE_VIEW(database, collection, id)){
          _ => true
        }.map{
          _.head
        }
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


  //Private Utility methods

  // if resultHandler is not specified, the identity function is used by implicits of Predef
  private def executeStmtsInTransaction[T](sql: String*)(implicit resultHandler: QueryResult => T) : Future[List[T]] = {

    def addResultToList(qr: QueryResult, l : List[T]) : List[T] = resultHandler(qr)::l

    def  sendstmt(stmt: String, results: List[T])(implicit c: Connection) : Future[List[T]]= {
      Logger.debug("SQL IN TRANSACTION: " + sql)
      c.sendQuery(stmt).map(qr => addResultToList(qr, results))
    }

    pool.inTransaction { implicit c =>
      sql.foldLeft ( Future.successful( List[T]() ) )
      {
        (futurelist: Future[List[T]], stmt: String) => futurelist.flatMap (l => sendstmt(stmt,l))
      }
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }
  }

  private def sendprepared[T](sql: String, vals: Seq[Any])(implicit c : Connection) : Future[QueryResult] = {
    Logger.debug( s"\t\t Executing Prepared statement $sql in transactions with values ${vals mkString ", "}" )
    c.sendPreparedStatement(sql, vals)
  }

  private def executePreparedStmts[T](sql: String, values: Seq[Seq[Any]])(implicit resultHandler: QueryResult => T) : Future[List[T]] =
    pool.inTransaction { implicit c =>
      values.foldLeft( Future.successful( List[T]() )) {
        (flist: Future[List[T]], vals: Seq[Any]) =>
          flist.flatMap{ l =>
            sendprepared(sql, vals)
              .map( resultHandler )
              .map(t => t::l)
          }
      }
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }


  private def executePreparedStmt[T](sql: String, values: Seq[Any])(implicit resultHandler: QueryResult => T) : Future[T] =
    pool.inTransaction { implicit c =>
      sendprepared(sql, values).map( resultHandler)
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }


  private def executeStmt[T](sql: String)(resultHandler: QueryResult => T): Future[T] = {
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

  private def jsArrayToJsPathList(arr: JsArray) : List[JsPath] =  arr.as[List[String]].map {
    spath => spath.split("\\.").foldLeft[JsPath]( __ )( (jsp, pe) => jsp \ pe )
  } ++ List(( __ \ "type"), ( __ \ "geometry"))

  private def first[T](rowF : RowData => Option[T])(qr: QueryResult) : Option[T] = qr.rows match {
    case Some(rs) if rs.size > 0 => rowF(rs.head)
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



    def SELECT_DATA(db: String, col: String, query: SpatialQuery, start: Option[Int] = None, limit: Option[Int] = None): String = {

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
       |SELECT json, ID
       |FROM ${quote(db)}.${quote(col)}
       |WHERE $cond
       |ORDER BY ID
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
          | $ExtentField JSON,
          | $IndexLevelField INT,
          | $CollectionField VARCHAR(255) PRIMARY KEY
          | )
       """.stripMargin

    def CREATE_VIEW_TABLE_IN(dbname: String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(ViewCollection)} (
          | COLLECTION VARCHAR,
          | VIEW_NAME VARCHAR,
          | VIEW_DEF JSON,
          | UNIQUE (COLLECTION, VIEW_NAME)
          | )
          |
       """.stripMargin


    def CREATE_COLLECTION_TABLE(dbname : String, tableName : String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(tableName)} (
          | id VARCHAR(255) PRIMARY KEY,
          | geometry GEOMETRY,
          | json JSON
          | )
       """.stripMargin

    def CREATE_COLLECTION_INDEX(dbname: String, tableName: String) =
      s"""CREATE INDEX ${quote(tableName + "_spatial_index")}
      | ON ${quote(dbname)}.${quote(tableName)} USING GIST ( geometry )
     """.stripMargin

    def INSERT_DATA(dbname: String, tableName: String) =
      s"""INSERT INTO ${quote(dbname)}.${quote(tableName)}  (id, json, geometry)
         |VALUES (?, ?, ?)
       """.stripMargin

    def DELETE_DATA(dbname: String, tableName: String, where: String) =
      s"""DELETE FROM ${quote(dbname)}.${quote(tableName)}
        WHERE ${where}
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
  }


  /**
   * And Extractor that translates a GenericDataseException into the appropratie database Exception
   */
  object MappableException {
    def getStatus(dbe : GenericDatabaseException) = dbe.errorMessage.fields.get('C')
    def getMessage(dbe: GenericDatabaseException) = dbe.errorMessage.fields.getOrElse('M', "Database didn't return a message")
    def unapply(t : Throwable): Option[RuntimeException] =  t match {
      case t: GenericDatabaseException if getStatus(t) == Some("42P06") =>
        Some(new DatabaseAlreadyExists(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("42P07") =>
        Some(new CollectionAlreadyExists(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("3F000") =>
        Some(new  DatabaseNotFoundException(getMessage(t)))
      case t: GenericDatabaseException if getStatus(t) == Some("42P01") =>
        Some(new CollectionNotFoundException(getMessage(t)))
      case _ => None
    }
  }


}
