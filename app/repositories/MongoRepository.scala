package repositories


import org.geolatte.geom.curve.MortonContext
import org.geolatte.geom.Envelope
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb._
import play.api.Logger
import org.geolatte.nosql.mongodb.Metadata
import scala.Some
import play.modules.reactivemongo.ReactiveMongoPlugin
import reactivemongo.core.commands.{Count, CreateCollection}
import scala.concurrent.Future
import reactivemongo.bson.BSONDocument
import reactivemongo.api.collections.default.BSONCollection
import play.api.libs.iteratee.Enumerator
import reactivemongo.api.FailoverStrategy

//TODO -- configure the proper execution context
import config.AppExecutionContexts.streamContext
import reactivemongo.api.collections.default.BSONCollectionProducer



/**
 * @author Karel Maesen, Geovise BVBA
 *
 */

//this needs to move to a service layer
object MongoRepository {


  import MetadataIdentifiers._

  //TODO -- make the client configurable
  import play.api.Play.current
  def driver = ReactiveMongoPlugin.driver
  def connection = ReactiveMongoPlugin.connection



  //TODO -- this is now hardcoded since ReactiveMongo apparently doesn't have a method to
  def listDatabases: Traversable[String] = List("nstest", "test", "tiger")

  def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def createDb(dbname: String): Future[Boolean] =
    if (existsDb(dbname)) Future.successful( false )
    else {
      val db = connection(dbname)
      val cmd = new CreateCollection(name = MetadataCollection, autoIndexId = Some(true))
      db.command( cmd )
    }

  def deleteDb(dbname: String): Future[Boolean] =
    if (existsDb(dbname) ) {
      connection(dbname).drop()
    }
    else Future.successful(false)

  def existsDb(dbname: String) : Boolean = listDatabases.exists( _ == dbname)


  def listCollections(dbname: String): Future[List[String]] =
    if (existsDb(dbname)) connection.db(dbname).collectionNames.map( _.filterNot(isMetadata(_)))
    else Future.failed(new NoSuchElementException(s"database $dbname doesn't exist"))


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


//  //TODO -- these operations don't catch exceptions. if exceptions need to be catched here, then replace Boolean return values
//  // with error statuses
//
//  def createCollection(dbName: String, colName: String, spatialSpec: Option[SpatialSpec]) : Boolean = {
//    if (! existsDb(dbName) || existsCollection(dbName, colName)) false
//    else {
//      //we give the capped option explicitly so that creation is not deferred!
//      val created = connection(dbName).createCollection(colName, MongoDBObject("capped" -> false))
//      Logger.info("Created: " + created.getFullName)
//      if (spatialSpec.isDefined) {
//        Logger.info(created.getFullName + " is spatially enabled")
//        val writeResult = connection(dbName)(MetadataCollection).insert(MongoDBObject(
//          ExtentField -> spatialSpec.get.envelope,
//          IndexLevelField  -> spatialSpec.get.level,
//          CollectionField -> colName
//        ), WriteConcern.Safe)
//        Logger.error(writeResult.getLastError().getErrorMessage)
//      }
//      true
//    }
//  }

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

  def deleteCollection(dbName: String, colName: String) : Future[Boolean] = {
    for {
      collectionExists <- existsCollection(dbName, colName)
      if collectionExists
    } yield {
      Logger.info(s"Starting removal of $dbName/$colName")
      //TODO -- when I don't specify FailoverStrategy explicitly, the scala compiler crashes!
      connection(dbName).collection(colName, FailoverStrategy()).drop()
      connection(dbName).collection(MetadataCollection, FailoverStrategy()).remove(BSONDocument( CollectionField -> colName) )
      Logger.info(s"Finalized removal of $dbName/$colName")
      true
    }
  }


  private def mkMortonContext (md: SpatialMetadata): MortonContext = new MortonContext (md.envelope, md.level)

}

