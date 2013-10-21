package nosql.mongodb

import org.geolatte.geom.Envelope
import play.api.Logger
import reactivemongo.core.commands._
import play.api.libs.iteratee._
import reactivemongo.api._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.concurrent.Future
import nosql.json.GeometryReaders._
import scala.util.Failure
import nosql.Exceptions.DatabaseDeleteException
import scala.Some
import nosql.Exceptions.DatabaseCreationException
import scala.util.Success
import nosql.Exceptions.DatabaseNotFoundException
import nosql.Exceptions.CollectionAlreadyExists
import play.modules.reactivemongo.json.collection.JSONCollection
import nosql.Exceptions.DatabaseAlreadyExists
import reactivemongo.core.commands.GetLastError
import reactivemongo.api.FailoverStrategy
import nosql.Exceptions.CollectionNotFoundException

import config.AppExecutionContexts.streamContext


/**
 * @author Karel Maesen, Geovise BVBA
 *
 */
object MongoRepository {

  val CREATED_DBS_COLLECTION = "createdDatabases"
  val CREATED_DB_PROP = "db"
  val DEFAULT_SYS_DB = "featureServerSys"

  import MetadataIdentifiers._
  import play.api.Play.current

  //TODO -- make the client configurable
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))

  private val systemDatabase = current.configuration.getString("fs.system.db").orElse(Some(DEFAULT_SYS_DB)).get

  private val createdDBColl = {
    connection.db(systemDatabase).collection[JSONCollection](CREATED_DBS_COLLECTION)
  }

  def listDatabases: Future[List[String]] = {
    val futureJsons = createdDBColl.find(Json.obj()).cursor[JsObject].collect[List]()
    futureJsons.map( _.map(
      json => (json \ CREATED_DB_PROP).as[String]))

  }

  def getMetadata(dbName: String) = connection.db(dbName).collection[JSONCollection](MetadataCollection)

  def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def createDb(dbname: String) = {

    //Logs the database creation in the "created databases" collection in the systemDatabase
    def registerDbCreation(dbname: String) = createdDBColl.save(Json.obj(CREATED_DB_PROP -> dbname)).andThen {
      case Success(le) => Logger.debug(s"Registering $dbname in created databases collections succeeded.")
      case Failure(le) => Logger.warn(s"Registering $dbname in created databases collections succeeded.")
    }

    existsDb(dbname).flatMap {
      case false => {
        val db = connection(dbname)
        val cmd = new CreateCollection(name = MetadataCollection, autoIndexId = Some(true))
        db.command(cmd) flatMap (_ => registerDbCreation(dbname)) recover {
          case ex: BSONCommandError => throw new DatabaseCreationException(s"Failure to create $dbname: can't create metadatacollection")
          case ex: Throwable => throw new DatabaseCreationException(s"Unknown exception of type: ${ex.getClass.getCanonicalName} having message: ${ex.getMessage}")
        }
      }
      case _ => throw new DatabaseAlreadyExists(s"Database $dbname already exists.")
    }

  }

  def deleteDb(dbname: String) = {

    def removeLog(dbname: String) = createdDBColl.remove(Json.obj(CREATED_DB_PROP -> dbname)).andThen {
      case Success(le) => Logger.info(s"Database $dbname dropped")
      case Failure(t) => Logger.warn(s"Database $dbname dropped, but could not register drop in $systemDatabase/$CREATED_DBS_COLLECTION. \nReason: ${t.getMessage}")
    }

    existsDb(dbname).flatMap {
      case true => connection(dbname).drop().flatMap(_ => removeLog(dbname)).recover {
        case ex : Throwable => {
          Logger.error(s"Problem deleting database $dbname", ex)
          throw new DatabaseDeleteException(s"Unknown exception of type: ${ex.getClass.getCanonicalName} having message: ${ex.getMessage}")
        }
      }
      case _ => throw new DatabaseNotFoundException(s"No database $dbname")
    }
  }

  def existsDb(dbname: String): Future[Boolean] = listDatabases.map(l => l.exists(_ == dbname))


  def listCollections(dbname: String): Future[List[String]] =
    existsDb(dbname).flatMap {
      case true => connection.db(dbname).collectionNames.map(_.filterNot(isMetadata(_)))
      case _ => throw new DatabaseNotFoundException(s"database $dbname doesn't exist")
    }


  def count(database: String, collection: String): Future[Int] = {
    val cmd = new Count(collection)
    connection.db(database).command(cmd)
  }

  def metadata(database: String, collection: String): Future[Metadata] = {
    import MetadataIdentifiers._
    import Metadata._

    def readMetadata() = {
      val metaCollection = connection(database).collection[JSONCollection](MetadataCollection)
      val metadataCursor = metaCollection.find( Json.obj(CollectionField -> collection)).cursor[JsObject]
      metadataCursor.headOption().map {
        case Some(doc) => doc.asOpt[Metadata]
        case _ => None
      }
    }

    def mkMetadata() = for {
        mdOpt <- readMetadata()
        cnt <- count(database, collection)
      } yield mdOpt.map( md => md.copy(count = cnt)).getOrElse( Metadata(collection, Envelope.EMPTY, 0, cnt) )

    existsDb(database).flatMap(existsDb =>
      if (existsDb) existsCollection(database, collection)
      else throw DatabaseNotFoundException()
    ).flatMap( existsCollection =>
      if(existsCollection) mkMetadata()
      else throw CollectionNotFoundException()
    )
  }

  //we check also the "hidden" names (e.g. metadata collection) so that puts will fail
  def existsCollection(dbName: String, colName: String): Future[Boolean] = for {
    names <- connection.db(dbName).collectionNames
    found = names.exists(_ == colName)
  } yield found


  def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]) = {

    def doCreateCollection() = connection(dbName).collection[JSONCollection](colName).create().andThen {
      case Success(b) => Logger.info(s"collection $colName created: $b")
      case Failure(t) => Logger.error(s"Attempt to create collection $colName threw exception: ${t.getMessage}")
    }

    def saveMetadata(specOpt: Option[Metadata]) = specOpt map {
      spec => connection(dbName).collection[JSONCollection](MetadataCollection).insert(Json.obj(
        ExtentField -> spec.envelope,
        IndexLevelField -> spec.level,
        CollectionField -> colName),
        GetLastError(awaitJournalCommit = true)
      ).andThen {
        case Success(le) => Logger.info(s"Writing metadata for $colName has result: ${le.ok}")
        case Failure(t) => Logger.error(s"Writing metadata for $colName threw exception: ${t.getMessage}")
      }.map(_ => true)
    } getOrElse { Future.successful(true) }

    existsDb(dbName).flatMap( dbExists =>
      if ( dbExists ) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap ( collectionExists =>
      if (!collectionExists) doCreateCollection()
      else throw new CollectionAlreadyExists()
    ).flatMap ( _ =>  saveMetadata(spatialSpec))

  }

  def deleteCollection(dbName: String, colName: String) = {

    def doDeleteCollection() = connection(dbName).collection[JSONCollection](colName, FailoverStrategy()).drop().andThen {
      case Success(b) => Logger.info(s"Deleting $dbName/$colName: $b")
      case Failure(t) => Logger.warn(s"Delete of $dbName/$colName failed: ${t.getMessage}")
    }

    def removeMetadata() = connection(dbName).collection[JSONCollection](MetadataCollection, FailoverStrategy()).remove(Json.obj(CollectionField -> colName)).andThen {
      case Success(le) => Logger.info(s"Removing metadata for $dbName/$colName: ${le.ok}")
      case Failure(t) => Logger.warn(s"Removing of $dbName/$colName failed: ${t.getMessage}")
    }

    existsDb(dbName).flatMap(dbExists =>
      if (dbExists) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap(collectionExists =>
      if (collectionExists) doDeleteCollection()
      else throw new CollectionNotFoundException()
    ).flatMap(_ => removeMetadata())

  }


  def getCollection(dbName: String, colName: String): Future[(JSONCollection, Metadata)] =
    existsDb(dbName).flatMap(dbExists =>
      if (dbExists) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap {
      case false => throw new CollectionNotFoundException()
      case true => {
        val col = connection(dbName).collection[JSONCollection](colName)
        metadata(dbName, colName).map(md => (col, md))
      }
    }

  def getSpatialCollection(database: String, collection: String) = {
    val coll = connection(database).collection[JSONCollection](collection)
    metadata(database, collection).map( md =>
      MongoSpatialCollection(coll, md)
    )
  }

  def query(database: String, collection: String, window: Envelope): Future[Enumerator[JsObject]] = {
    getSpatialCollection(database, collection).map(_.query(window))
  }

  def getData(database: String, collection: String): Future[Enumerator[JsObject]] = {
    getSpatialCollection(database, collection).map(_.out())
  }



}

