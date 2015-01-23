package nosql.postgresql

import com.github.mauricio.async.db.{QueryResult, Connection, ResultSet, RowData}
import com.github.mauricio.async.db.pool.{PoolConfiguration, ConnectionPool}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import config.{AppExecutionContexts, ConfigurationValues}
import nosql._
import nosql.json.GeometryReaders
import org.geolatte.geom.codec.{Wkt, Wkb}
import org.geolatte.geom.{Polygon, Envelope}
import play.api.Logger
import play.api.libs.iteratee.{Iteratee, Enumerator}
import querylang.{QueryParser, BooleanExpr}
import utilities.JsonHelper

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * A NoSQL feature store repository for Postgresql/Postgis
 *
 * Created by Karel Maesen, Geovise BVBA on 11/12/14.
 */
object PostgresqlRepository extends Repository {

  import AppExecutionContexts.streamContext
  import play.api.libs.json._
  import GeometryReaders._

  //Note that we cannot use preparedstatements for DDL's


  //TODO -- should we check for quotes, spaces etc.  in str?
  // alternative is to use escapeSql in the org.apache.commons.lang.StringEscapeUtils
  private def quote(str: String) : String = "\"" + str + "\""
  private def single_quote(str: String) : String = "'" + str + "'"


  private def parseSelector(expression : Option[BooleanExpr]) = expression match {
    //TODO -- this is just a placeholder
    case __ => "TRUE"
  }

  //These are the SQL statements for managing and retrieving data
  object Sql {

   def condition(query : SpatialQuery) : String = {
     val windowCondition = query.windowOpt match {
       case Some(env) => s"geometry && ${single_quote( Wkt.toWkt(FeatureTransformers.toPolygon(env)))}::geometry"
       case _ => "TRUE"
     }
     val attCondition = parseSelector(query.queryOpt)
     List(windowCondition, attCondition) mkString " AND "
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
       |SELECT ID, json
       |FROM ${quote(db)}.${quote(col)}
       |WHERE $cond
       |ORDER BY ID
     """.stripMargin + offsetClause + limitClause

    }


    def UPDATE_DATA(db: String, col: String) : String = {
      s"""UPDATE ${quote(db)}.${quote(col)}
         |SET json = ?, geometry = ?
         |WHERE ?
       """.stripMargin
    }

    import MetadataIdentifiers._

    val LIST_SCHEMA = "select schema_name from information_schema.schemata"

    def CREATE_SCHEMA(dbname: String) = s"create schema ${quote(dbname)}"

    def DROP_SCHEMA(dbname : String) : String = s"drop schema ${quote(dbname)} CASCADE"

    def CREATE_METADATA_TABLE_IN(dbname : String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(MetadataCollection)} (
          | $ExtentField JSON,
          | $IndexLevelField INT,
          | $CollectionField VARCHAR(255) PRIMARY KEY
          | )
       """.stripMargin

    def CREATE_COLLECTION_TABLE(dbname : String, tableName : String) =
      s"""CREATE TABLE ${quote(dbname)}.${quote(tableName)} (
          | id INT PRIMARY KEY,
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

    def DELETE_DATA(dbname: String, tableName: String) =
    s"""DELETE FROM ${quote(dbname)}.${quote(tableName)}
     """.stripMargin


    def LIST_TABLE_NAMES(dbname: String) = {
      s""" select table_name from information_schema.tables
         | where
         | table_schema = ${single_quote(dbname)}
         | and table_type = 'BASE TABLE'
         | and table_name != ${quote(MetadataCollection)}
       """.stripMargin
    }

    def INSERT_Metadata(dbname: String, tableName: String, md: Metadata) =
    s"""insert into ${quote(dbname)}.${quote(MetadataCollection)} values(
       | ${single_quote(Json.stringify(Json.toJson(md.envelope)))}::json,
       | ${md.level},
       | ${single_quote(tableName)}
       | )
     """.stripMargin

    def SELLECT_COLLECTION_NAMES(dbname: String) =
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

  }


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

  lazy val url = {
    val parsed = URLParser.parse(ConfigurationValues.PgConnectionString)
    Logger.info("Connecting to postgresql database with URL : " + parsed)
    parsed
  }

  lazy val database = url.database.getOrElse(url.username)
  lazy val factory = new PostgreSQLConnectionFactory( url )

  //TODO make connection pool configurable
  lazy val pool = new ConnectionPool(factory, PoolConfiguration.Default)

  override def createDb(dbname: String): Future[Boolean] =
    pool.inTransaction { c => {
      c.sendQuery(s"${Sql.CREATE_SCHEMA(dbname)}; ${Sql.CREATE_METADATA_TABLE_IN(dbname)};")
    }.map {
      _ => true
    }.recover {
      case MappableException(mappedException) => throw mappedException
      case _ @ t
        =>  throw new DatabaseCreationException(s"Unknown exception having message: ${t.getMessage}")
      }
    }

  override def listDatabases: Future[List[String]] =
    pool.sendQuery(Sql.LIST_SCHEMA)
      .map{ qr => {
        qr.rows.map( rs => rs.foldLeft(List[String]())( (ac, data) => data(0).asInstanceOf[String]::ac))
        }.getOrElse(List[String]())
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

  override def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]): Future[Boolean] =
    pool.inTransaction { c =>
      c.sendQuery(
        Sql.CREATE_COLLECTION_TABLE(dbName, colName)
      ).flatMap { _ =>
        spatialSpec match {
          case Some(md) =>
            c.sendQuery(Sql.INSERT_Metadata(dbName, colName, md))
             .map(_ => true)
          case _ => Future.successful(true)
        }
      }.flatMap { _ =>
          c.sendQuery(Sql.CREATE_COLLECTION_INDEX(dbName, colName))
      }.map{ _=>
        true
      }
        .recover {
        case MappableException(mappedException) => throw mappedException
      }
    }


  override def listCollections(dbname: String): Future[List[String]] =
    pool.sendQuery( Sql.SELLECT_COLLECTION_NAMES(dbname) ).map{ qr =>
      qr.rows match {
        case Some(rs) =>
          rs.foldLeft(List[String]()) ( (acc, row)  => row(0).asInstanceOf[String] :: acc )
        case _ => List[String]()
      }
    }.recover {
      case MappableException(mappedException) => throw mappedException
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

    count(database, collection)
      .flatMap { cnt =>
        pool.sendQuery(Sql.SELECT_METADATA(database, collection))
          .map { qr =>
          qr.rows match {
            case Some(rs) if rs.size > 0 => mkMetadata(rs.head, cnt)
            case _ => throw new CollectionNotFoundException()
          }
        }
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }

  }

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] =
  pool.inTransaction { c =>
    c.sendQuery {
      Sql.DELETE_METADATA(dbName, colName)
    }.flatMap { _ =>
      c.sendQuery {
        Sql.DROP_TABLE(dbName, colName)
      }.map ( _ => true)
    }
  }.recover {
    case MappableException(mappedException) => throw mappedException
  }

  override def count(database: String, collection: String): Future[Long] =
    pool.sendQuery(Sql.SELECT_COUNT(database, collection))
      .map { qr =>
      qr.rows match {
        case Some(rs) if rs.size > 0 => rs.head(0).asInstanceOf[Long]
        case _ => throw new CollectionNotFoundException()
      }
    }

  override def existsCollection(dbName: String, colName: String): Future[Boolean] =
    pool.sendQuery(Sql.SELLECT_COLLECTION_NAMES(dbName))
      .map { qr =>
      qr.rows match {
        case Some(rs) =>
          rs.filter( row => row(0).asInstanceOf[String].equalsIgnoreCase(colName)).nonEmpty
        case _ => throw new RuntimeException("Query failed to return arow set")
      }
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }



  override def writer(database: String, collection: String): FeatureWriter = new PGWriter(database, collection)



  def insert(database: String, collection: String, jsons: Seq[(JsObject,Polygon)] ): Future[Long] = {

    def chain(first : Future[Long], second: => Future[Long]) : Future[Long]= first.flatMap( r => second.map(s => r + s))

    def insertInner(c: Connection, obj: JsObject, env: Polygon) : Future[Long] = c.sendPreparedStatement(Sql.INSERT_DATA(database, collection),
      Array((obj \ "id").as[Int], obj, org.geolatte.geom.codec.Wkb.toWkb(env)))
      .map(res => res.rowsAffected)

    pool.inTransaction { c =>
      jsons.foldLeft(Future.successful(0L)) {
        case (acc, (obj, env)) => chain(acc, insertInner(c, obj, env))
      }
    }
  }

  def insert(database: String, collection: String, json: JsObject, env: Polygon ): Future[Boolean] =
    pool.inTransaction { c =>
      //TODO -- clean-up id en envelope to wkb extraction
       c.sendPreparedStatement(Sql.INSERT_DATA(database, collection), Array((json \ "id").as[Int], json, org.geolatte.geom.codec.Wkb.toWkb(env) ))
          .map(_ => true)
    }.recover {
      case MappableException(mappedException) => throw mappedException
    }

  private def toJson(id: Int, text : String) : Option[JsObject] =
    Json
      .parse(text)
      .asOpt[JsObject]

  private def jsArrayToJsPathList(arr: JsArray) : List[JsPath] =  arr.as[List[String]].map {
    spath => spath.split("\\.").foldLeft[JsPath]( __ )( (jsp, pe) => jsp \ pe )
  } ++ List(( __ \ "type"), ( __ \ "geometry"))


  private def enumerate(rs : ResultSet, optProj : Option[Reads[JsObject]]) : Enumerator[JsObject] = {

    val jsons = rs.map {
      rd => toJson(rd(0).asInstanceOf[Int], rd(1).asInstanceOf[String])
    }

    val projected = optProj match {
      case Some(reads) => jsons.map{
        jsOpt => jsOpt.flatMap( js => js.asOpt(reads))
      }
      case _ => jsons
    }

    val collected = projected.collect {
      case Some(js) => js
    }

    Enumerator.enumerate(collected)
  }

  override def query(database: String, collection: String, spatialQuery: SpatialQuery, start : Option[Int] = None,
                     limit: Option[Int] = None): Future[CountedQueryResult] = {

    //a utility to extract the count from the QueryResult
    def extractCount(qr: QueryResult) : Long =
      qr.rows match {
        case Some(rowSet) => rowSet.head(0).asInstanceOf[Long]
        case _ => 0
      }

    val projectingReads : Option[Reads[JsObject]] =
      spatialQuery.projectionOpt
        .map {
        arr => jsArrayToJsPathList(arr)
      }.map {
        pathList => JsonHelper.mkProjection(pathList)
      }

    val fCnt = pool.sendQuery(Sql.SELECT_TOTAL_IN_QUERY(database, collection, spatialQuery)).map{ extractCount }

    val fEnum = pool.sendQuery(Sql.SELECT_DATA(database, collection, spatialQuery, start, limit)).map {
        qr => qr.rows match {
          case Some(rs) => enumerate(rs,projectingReads)
          case _ => Enumerator[JsObject]()
      }
    }

    {for{
      cnt <- fCnt
      enum <- fEnum
    } yield
      (Some(cnt),enum)
    } recover {
      case MappableException(mappedException) => throw mappedException
    }
  }

  override def delete(database: String, collection: String, query: BooleanExpr): Future[Boolean] =
    pool.sendQuery(Sql.DELETE_DATA(database, collection))
      .map(_ => true)
      .recover {
      case MappableException(mappedException) => throw mappedException
    }

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] =
    metadata(database, collection)
      .map{ md =>
        FeatureTransformers.envelopeTransformer(md.envelope)
      }.flatMap { evr =>
        insert(database, collection, json, json.as[Polygon](evr))
    }

  def update(database: String, collection: String, query: BooleanExpr, newValue: JsObject, envelope: Polygon) : Future[Int] =
    pool.sendPreparedStatement( Sql.UPDATE_DATA(database, collection), Array(newValue, Wkb.toWkb(envelope), ""))
      .map { res =>
        res.rowsAffected.toInt
    }

  override def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int] =
    metadata(database, collection)
      .map { md => FeatureTransformers.envelopeTransformer(md.envelope)
    }.flatMap { implicit evr => {
        val ne = updateSpec.as[Polygon]
        update(database, collection, query, updateSpec, ne)
      }
    }

  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] = {
    val idq = s" id = ${(json \ "id").as[Int]}"

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
  override def saveView(database: String, collection: String, viewDef: JsObject): Future[Boolean] = ???

  override def dropView(database: String, collection: String, id: String): Future[Boolean] = ???



  override def existsDb(dbname: String): Future[Boolean] = ???

  override def getView(database: String, collection: String, id: String): Future[JsObject] = ???



  override def getViews(database: String, collection: String): Future[List[JsObject]] = ???

}
