package nosql.postgresql

import com.github.mauricio.async.db.RowData
import com.github.mauricio.async.db.pool.{PoolConfiguration, ConnectionPool}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import config.{AppExecutionContexts, ConfigurationValues}
import nosql._
import nosql.json.GeometryReaders
import org.geolatte.geom.Envelope
import play.api.Logger
import play.api.libs.iteratee.Enumerator

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

  //Note that we cannot use preparedstatements for DDL's


  //TODO -- should we check for quotes, spaces etc.  in str?
  // alternative is to use escapeSql in the org.apache.commons.lang.StringEscapeUtils
  private def quote(str: String) : String = "\"" + str + "\""
  private def single_quote(str: String) : String = "'" + str + "'"


  //These are the SQL statements for managing and retrieving data
  object Sql {

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
          | id SERIAL PRIMARY KEY,
          | geometry GEOMETRY,
          | json JSON
          | )
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
    
  lazy val url = URLParser.parse(ConfigurationValues.PgConnectionString)
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
      }.recover {
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
//      case _@t
//      => throw new RuntimeException(s"Unknown exception having message: ${t.getMessage}")
    }

  }

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] =
  pool.inTransaction { c =>
    c.sendQuery {
      println("DELETE METADATA :====: " + Sql.DELETE_METADATA(dbName, colName))
      Sql.DELETE_METADATA(dbName, colName)
    }.flatMap { _ =>
      c.sendQuery {
        println("DELETE METADATA :====: " + Sql.DROP_TABLE(dbName, colName))
        Sql.DROP_TABLE(dbName, colName)
      }.map ( _ => true)
    }
  }.recover {
    case MappableException(mappedException) => throw mappedException
//    case _ @ t
//    =>  throw new RuntimeException(s"Unknown exception having message: ${t.getMessage}")
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


  override def query(database: String, collection: String, spatialQuery: SpatialQuery): Future[Enumerator[JsObject]] = ???

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

  override def update(database: String, collection: String, query: JsObject, updateSpec: JsObject): Future[Int] = ???

  override def writer(database: String, collection: String): FeatureWriter = ???

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] = ???

  override def existsDb(dbname: String): Future[Boolean] = ???

  override def getView(database: String, collection: String, id: String): Future[JsObject] = ???

  override def delete(database: String, collection: String, query: JsObject): Future[Boolean] = ???

  override def getViews(database: String, collection: String): Future[List[JsObject]] = ???




  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] = ???


}
