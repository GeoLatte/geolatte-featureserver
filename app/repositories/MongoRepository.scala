package repositories


import util.SpatialSpec
import org.geolatte.geom.curve.MortonContext
import org.geolatte.geom.Envelope
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb._
import play.api.Logger
import org.geolatte.nosql.mongodb.Metadata
import scala.Some
import reactivemongo.core.commands.{GetLastError, Count, CreateCollection}
import scala.concurrent._
import reactivemongo.bson.{BSONString, BSONDocument}
import reactivemongo.api.collections.default.BSONCollection
import play.api.libs.iteratee.Enumerator
import reactivemongo.api.{MongoDriver, FailoverStrategy}

import scala.util.{Failure, Success}

//TODO -- configure the proper execution context
import config.AppExecutionContexts.streamContext
import reactivemongo.api.collections.default.BSONCollectionProducer


/**
 * @author Karel Maesen, Geovise BVBA
 *
 */

//TODO this needs to move to a service layer
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
    connection.db(systemDatabase).collection(CREATED_DBS_COLLECTION)
  }

  def listDatabases: Future[List[String]] = {
    val futureBsons = createdDBColl.find(BSONDocument()).cursor.collect[List]()
    futureBsons.map(_.map(
      bson => bson.getAs[String](CREATED_DB_PROP)).flatten
    )
  }

  def getMetadata(dbName: String) = connection.db(dbName).collection(MetadataCollection)

  def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def createDb(dbname: String): Future[Boolean] = {

    def logCreation(dbname: String) = {
      val fle = createdDBColl.save(BSONDocument( CREATED_DB_PROP -> BSONString(dbname)))
      fle onComplete {
        case Success(le) => if (!le.ok) Logger.warn(s"Database ${dbname} created, but could not register in ${systemDatabase}/${CREATED_DBS_COLLECTION}. \nReason: ${le.errMsg.getOrElse("<No Message>")}")
        case Failure(t) => Logger.warn(s"Database ${dbname} created, but could not register in ${systemDatabase}/${CREATED_DBS_COLLECTION}. \nReason: ${t.getMessage}")
      }
    }

    existsDb(dbname).flatMap {
      case false => {
        val db = connection(dbname)
        val cmd = new CreateCollection(name = MetadataCollection, autoIndexId = Some(true))
        db.command(cmd).andThen { case Success(b) if b  => logCreation(dbname) }
      }
      case _ => Future.successful(false)
    }

  }

  def deleteDb(dbname: String): Future[Boolean] = {

    def removeLog(dbname: String) = {
      val fle = createdDBColl.remove(BSONDocument(CREATED_DB_PROP -> BSONString(dbname)))
      fle onComplete {
        case Success(le) => if (!le.ok) Logger.warn(s"Database ${dbname} dropped, but could not register drop in ${systemDatabase}/${CREATED_DBS_COLLECTION}. \nReason: ${le.errMsg.getOrElse("<No Message>")}")
        case Failure(t) => Logger.warn(s"Database ${dbname} dropped, but could not register drop in ${systemDatabase}/${CREATED_DBS_COLLECTION}. \nReason: ${t.getMessage}")
      }
    }

    existsDb(dbname).flatMap {
      case true => connection(dbname).drop().andThen { case Success(resp) if resp => removeLog(dbname) }
      case _  => Future.successful(false)
    }
  }

  def existsDb(dbname: String) : Future[Boolean] = listDatabases.map(l => l.exists( _ == dbname) )


  def listCollections(dbname: String): Future[List[String]] =
    existsDb(dbname). flatMap {
      case true => connection.db(dbname).collectionNames.map( _.filterNot(isMetadata(_)))
      case _ => Future.failed(new NoSuchElementException(s"database $dbname doesn't exist"))
    }


  def count(database: String, collection: String) : Future[Int] = {
    val cmd = new Count( collection )
    connection.db(database).command(cmd)
  }

  def metadata(database: String, collection: String): Future[Metadata] = {
    import MetadataIdentifiers._

    val metaCollection : BSONCollection = connection(database).collection(MetadataCollection)
    val metadataCursor = metaCollection.find(BSONDocument(CollectionField -> collection)).cursor[BSONDocument]

    val futureSmd = metadataCursor.headOption().map {
      _ match {
        case Some(doc) => SpatialMetadata.from(doc)
        case _ => None
      }
    }
    val futureCnt = count(database, collection)
    for {
      smd <- futureSmd
      cnt <- futureCnt
    } yield Metadata(collection, cnt,smd)
  }

  //we check also the "hidden" names (e.g. metadata collection) so that puts will fail
  def existsCollection(dbName: String, colName: String) : Future[Boolean] = for {
    names <- connection.db(dbName).collectionNames
    found = names.exists( _ == colName)
  } yield found


  def createCollection(dbName: String, colName: String, spatialSpec: Option[SpatialSpec]): Future[Boolean] = {

    def doCreateCollection() = connection(dbName).collection(colName).create().andThen {
      case Success(b) => Logger.info(s"collection ${colName} created: ${b}")
      case Failure(t) => Logger.info(s"Attempt to create collection ${colName} threw exception: ${t.getMessage}")
    }

    def saveMetadata(spec: SpatialSpec) = connection(dbName).collection(MetadataCollection).insert(BSONDocument(
        ExtentField -> spec.envelope,
        IndexLevelField -> spec.level,
        CollectionField -> colName),
        GetLastError(awaitJournalCommit = true)
      ).andThen {
        case Success(le) => Logger.info(s"Writing metadata for ${colName} has result: ${le.ok}")
        case Failure(t) => Logger.warn(s"Writing metadata for ${colName} threw exception: ${t.getMessage}")
      }.map(le => le.ok).recover { case t: Throwable => false }


    spatialSpec match {
      case Some(spec) => doCreateCollection.flatMap(res => if (res) saveMetadata(spec) else Future.successful(res))
      case None => doCreateCollection()
    }
  }

  def deleteCollection(dbName: String, colName: String) : Future[Boolean] = {

    def dropColl() = connection(dbName).collection(colName, FailoverStrategy()).drop().andThen{
      case Success(b) => Logger.info(s"Deleting ${dbName}/${colName}: ${b}")
      case Failure(t) => Logger.warn(s"Delete of ${dbName}/${colName} failed: ${t.getMessage}")
    }

    def removeMetadata() = connection(dbName).collection(MetadataCollection, FailoverStrategy()).remove(BSONDocument( CollectionField -> colName) ).andThen {
      case Success(le) => Logger.info(s"Removing metadata for ${dbName}/${colName}: ${le.ok}")
      case Failure(t) => Logger.warn(s"Removing of ${dbName}/${colName} failed: ${t.getMessage}")
    }.map(le => le.ok)

    dropColl().flatMap( res => if(res) removeMetadata() else Future.successful(false)).recover{ case t : Throwable => false }

  }


  def getCollection(dbName: String, colName: String) : Future[ (Option[BSONCollection], Option[SpatialMetadata]) ]  =
  existsCollection(dbName, colName).flatMap {
    case false => Future{(None, None)}
    case true => {
      val col : BSONCollection = connection(dbName).collection[BSONCollection](colName)
      metadata(dbName, colName).map(md => (Some(col), md.spatialMetadata))

    }
  }

  def getSpatialCollectionSource(database: String, collection: String) = {
    val futureMd = metadata(database, collection)
    val coll = connection(database).collection(collection)
    futureMd.map( md => MongoDbSource(coll, mkMortonContext( md.spatialMetadata.get )))
  }

  def query(database: String, collection: String, window: Envelope): Future[Enumerator[Feature]] = {
    getSpatialCollectionSource(database, collection).map(_.query(window))
  }

  def getData(database: String, collection: String): Future[Enumerator[Feature]] = {
    getSpatialCollectionSource(database, collection).map(_.out())
  }

//  def reindex(database: String, collection: String, level: Int) :Result = {
//    MongoRepository.getCollection(database, collection) match {
//              //TODO -- HTTP result codes in repository breaks layered design
//              case (None, _) => NotFound(s"$database/$collection does not exist.")
//              case (_ , None) => BadRequest(s"Can't reindex non-spatial collection.")
//              case (Some(coll), Some(smd)) =>
//                doReindex(coll, smd, level)
//                Ok("Reindex succeeded.")
//            }
//  }
//
//  private def doReindex(implicit collection: MongoCollection, smd: SpatialMetadata, level: Int) = {
  //    val mc = new MortonContext(smd.envelope, level)
  //    val newMortonCode = new MortonCode(mc)
  //    collection.map( obj => updateSpatialMetadata(obj, newMortonCode))
  //    updateSpatialMetadata(level)
  //  }
  //
  //  private def updateSpatialMetadata(newLevel: Int)(implicit coll: MongoCollection) = {
  //    import MetadataIdentifiers._
  //    val mdColl = coll.getDB().getCollection(MetadataCollection)
  //    mdColl.update( MongoDBObject( CollectionField -> coll.name) , $set(Seq(IndexLevelField -> newLevel)))
  //  }
  //
  //  private def updateSpatialMetadata(obj: MongoDBObject, newMortonCode : MortonCode)(implicit coll: MongoCollection) = {
  //    import SpecialMongoProperties._
  //    (for {
  //      feature <- MongoDbFeature.toFeature(obj)
  //      newMcVal = newMortonCode ofGeometry feature.getGeometry
  //    } yield newMcVal) match {
  //      case Some(m) => coll.update( MongoDBObject("_id" -> obj._id.get), $set(Seq(MC -> m)))
  //      case None => Logger.warn("During reindex, object with id %s failed" format obj._id.get)
  //    }
  //  }



  private def mkMortonContext (md: SpatialMetadata): MortonContext = new MortonContext (md.envelope, md.level)

}

