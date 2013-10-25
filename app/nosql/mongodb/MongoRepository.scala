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
import scala.Some
import scala.util.Success
import play.modules.reactivemongo.json.collection.JSONCollection
import reactivemongo.core.commands.GetLastError
import reactivemongo.api.FailoverStrategy
import nosql.Exceptions._
import reactivemongo.api.gridfs._
import reactivemongo.bson._

import config.AppExecutionContexts.streamContext
import scala.language.implicitConversions


case class Media(id: String, md5: Option[String])

case class MediaReader(id: String,
                       md5: Option[String],
                       name: String,
                       len: Int,
                       contentType: Option[String],
                       data: Array[Byte])

trait Repository {

  /**
   *The type returned by the database driver when reporting success or failure
   */
  type ReturnVal

  type SpatialCollection

  type MediaStore

  def listDatabases: Future[List[String]]

  def getMetadata(dbName: String) : JSONCollection

  def createDb(dbname: String) : Future[ReturnVal]

  def dropDb(dbname: String) : Future[ReturnVal]

  def existsDb(dbname: String): Future[Boolean]

  def listCollections(dbname: String): Future[List[String]]

  def count(database: String, collection: String) : Future[Int]

  def metadata(database: String, collection: String): Future[Metadata]

  def existsCollection(dbName: String, colName: String): Future[Boolean]

  def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]) : Future[Boolean]

  def deleteCollection(dbName: String, colName: String) : Future[ReturnVal]

  def getCollection(dbName: String, colName: String): Future[(JSONCollection, Metadata)]

  def getSpatialCollection(database: String, collection: String) : Future[SpatialCollection]

  def query(database: String, collection: String, window: Envelope): Future[Enumerator[JsObject]]

  def getData(database: String, collection: String): Future[Enumerator[JsObject]]

  def getMediaStore(database: String, collection: String): Future[MediaStore]

  def saveMedia(database: String, collection: String, producer: Enumerator[Array[Byte]], fileName: String, contentType: Option[String]) : Future[Media]

  def getMedia(database: String, collection: String, id: String) : Future[MediaReader]

}

object MongoRepository extends Repository {

  import scala.collection.JavaConversions._
  type ReturnVal = LastError
  type SpatialCollection = MongoSpatialCollection
  type MediaStore = GridFS[BSONDocument, BSONDocumentReader, BSONDocumentWriter]

  val CREATED_DBS_COLLECTION = "createdDatabases"
  val CREATED_DB_PROP = "db"
  val DEFAULT_SYS_DB = "featureServerSys"

  object CONFIGURATION {
    val MONGO_CONNECTION_STRING = "fs.mongodb"
    val MONGO_SYSTEM_DB = "fs.system.db"
  }

  import MetadataIdentifiers._
  import play.api.Play.current

  val driver = new MongoDriver
  val dbconStr = current.configuration.getStringList(CONFIGURATION.MONGO_CONNECTION_STRING)
    .getOrElse[java.util.List[String]](List("localhost"))

  val connection = driver.connection(dbconStr)

  private val systemDatabase = current.configuration.getString(CONFIGURATION.MONGO_SYSTEM_DB).orElse(Some(DEFAULT_SYS_DB)).get

  private val createdDBColl = {
    connection.db(systemDatabase).collection[JSONCollection](CREATED_DBS_COLLECTION)
  }

  def listDatabases: Future[List[String]] = {
    val futureJsons = createdDBColl.find(Json.obj()).cursor[JsObject].collect[List]()
    futureJsons.map( _.map(
      json => (json \ CREATED_DB_PROP).as[String]))

  }

  def getMetadata(dbName: String) = connection.db(dbName).collection[JSONCollection](MetadataCollection)

  private def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

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

  def dropDb(dbname: String) = {

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

  def getMediaStore(database: String, collection: String) : Future[MediaStore] = {
    import reactivemongo.api.collections.default._
    existsCollection(database, collection).map( exists =>
        if (exists) new GridFS(connection(database), "fs." + collection)(BSONCollectionProducer)
        else throw CollectionNotFoundException()
    )
  }

  private def bson2String(bson: BSONValue): String = bson match {
    case bi: BSONObjectID => bi.stringify
    case bi: BSONString => bi.value
    case _ => bson.toString
  }

  def saveMedia(database: String, collection: String, producer: Enumerator[Array[Byte]], fileName: String,
                contentType: Option[String]): Future[Media] = {
    import reactivemongo.api.gridfs.Implicits._
    import reactivemongo.bson._

    getMediaStore(database, collection).flatMap { gridFs =>
        gridFs.save(producer, DefaultFileToSave(filename = fileName, contentType = contentType, id = BSONObjectID.generate))
          .map( fr => Media(bson2String(fr.id), fr.md5))
    }
  }

  def getMedia(database: String, collection: String, id: String): Future[MediaReader] = {
    import reactivemongo.api.gridfs.Implicits._
    getMediaStore(database, collection).flatMap {
      gridFs => {
        Logger.info(s"Media object with id is searched: $id")
        gridFs.find(BSONDocument("_id" -> new BSONObjectID(id)))
          .headOption
          .filter(_.isDefined)
          .flatMap(file =>
            (gridFs.enumerate(file.get) |>>> Iteratee.fold[Array[Byte], Array[Byte]](Array[Byte]())((accu, part) =>  accu ++ part ))
            .map(binarydata => MediaReader(bson2String(file.get.id), file.get.md5, file.get.filename, file.get.length, file.get.contentType, binarydata))
        ).recover {
          case ex : Throwable => {
            Logger.warn(s"Media retrieval failed with exception ${ex.getMessage} of type: ${ex.getClass.getCanonicalName}")
            throw new MediaObjectNotFoundException()
          }
        }
      }
    }
  }

}

