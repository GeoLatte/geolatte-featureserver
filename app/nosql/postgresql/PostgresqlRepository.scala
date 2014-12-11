package nosql.postgresql

import com.github.mauricio.async.db.pool.{PoolConfiguration, ConnectionPool}
import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.github.mauricio.async.db.postgresql.messages.backend.InformationMessage
import com.github.mauricio.async.db.postgresql.pool.PostgreSQLConnectionFactory
import com.github.mauricio.async.db.postgresql.util.URLParser
import config.{AppExecutionContexts, ConfigurationValues}
import nosql.Exceptions.DatabaseAlreadyExists
import nosql.{FeatureWriter, Metadata, SpatialQuery, Repository}
import play.api.Play
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.JsObject

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 11/12/14.
 */
object PostgresqlRepository extends Repository {

  import AppExecutionContexts.streamContext

  object Sql {
    val LIST_SCHEMA = "select schema_name from information_schema.schemata"
    val CREATE_SCHEMA = "create schema "
    val DROP_SCHEMA = "drop schema "
    val Update = "UPDATE messages SET content = ?, moment = ? WHERE id = ?"
    val Select = "SELECT id, content, moment FROM messages ORDER BY id asc"
    val SelectOne = "SELECT id, content, moment FROM messages WHERE id = ?"
  }

  lazy val url = URLParser.parse(ConfigurationValues.PgConnectionString)
  lazy val database = url.database.getOrElse(url.username)
  lazy val factory = new PostgreSQLConnectionFactory( url )
  lazy val pool = new ConnectionPool(factory, PoolConfiguration.Default)

  //TODO -- should we check for quotes in str?
  private def quote(str: String) : String = "\"" + str + "\""

  override def createDb(dbname: String): Future[Boolean] =
    pool.inTransaction { c =>
        c.sendQuery(Sql.CREATE_SCHEMA + quote(dbname))
          .map { _ => true }
          .recover {
          case t : GenericDatabaseException => if(t.errorMessage.fields.get('C') == Some("42P06")) throw new DatabaseAlreadyExists() else throw t;
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
        c.sendQuery(Sql.DROP_SCHEMA + quote(dbname))
          .map(_ => true )
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

  override def deleteCollection(dbName: String, colName: String): Future[Boolean] = ???

  override def count(database: String, collection: String): Future[Int] = ???

  override def listCollections(dbname: String): Future[List[String]] = ???

  override def dropView(database: String, collection: String, id: String): Future[Boolean] = ???

  override def update(database: String, collection: String, query: JsObject, updateSpec: JsObject): Future[Int] = ???

  override def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]): Future[Boolean] = ???

  override def writer(database: String, collection: String): FeatureWriter = ???

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean] = ???

  override def existsDb(dbname: String): Future[Boolean] = ???

  override def metadata(database: String, collection: String): Future[Metadata] = ???



  override def getView(database: String, collection: String, id: String): Future[JsObject] = ???

  override def delete(database: String, collection: String, query: JsObject): Future[Boolean] = ???

  override def getViews(database: String, collection: String): Future[List[JsObject]] = ???

  override def existsCollection(dbName: String, colName: String): Future[Boolean] = ???



  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] = ???

  override def query(database: String, collection: String, spatialQuery: SpatialQuery): Future[Enumerator[JsObject]] = ???
}
