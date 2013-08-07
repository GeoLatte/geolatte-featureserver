package repositories


import org.geolatte.geom.curve.MortonContext
import org.geolatte.geom.Envelope
import com.mongodb.casbah.{MongoCollection, WriteConcern, MongoClient}
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{DBCollection, DBObject}
import util.SpatialSpec
import play.api.Logger
import org.geolatte.nosql.mongodb.Metadata
import util.SpatialSpec
import scala.Some

/**
 * @author Karel Maesen, Geovise BVBA
 *
 */

//this needs to move to a service layer
object MongoRepository {


  import MetadataIdentifiers._

  //TODO -- make the client configurable
  val mongo = MongoClient()


  def listDatabases(): Traversable[String] = mongo.dbNames

  def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def createDb(dbname: String): Boolean =
    if (existsDb(dbname)) false
    else {
      mongo(dbname).createCollection(MetadataCollection, MongoDBObject.empty)
      true
    }

  def deleteDb(dbname: String): Boolean =
    if (existsDb(dbname) ) {
      mongo(dbname).dropDatabase()
      true
    }
    else false

  def existsDb(dbname: String) : Boolean = mongo.dbNames.exists( _ == dbname)


  def listCollections(dbname: String): Option[Traversable[String]] =
    if (existsDb(dbname)) Some(mongo.getDB(dbname).collectionNames.filterNot(isMetadata(_)))
    else None


  def count(database: String, collection: String) : Long = mongo(database)(collection).count()

  def metadata(database: String, collection: String): Option[Metadata] = {
    import MetadataIdentifiers._

    val spatialMetadata = for {
      doc <- mongo(database)(MetadataCollection).findOne(MongoDBObject(CollectionField -> collection))
      td <- SpatialMetadata.from(doc)
    } yield td

    val cnt = count(database, collection)
    if (listCollections(database).getOrElse(List[String]()).exists(_ == collection)) Some(Metadata(collection, cnt,spatialMetadata))
    else None

  }

  //we check also the "hidden" names (e.g. metadata collection) so that puts will fail
  def existsCollection(dbName: String, colName: String) : Boolean = mongo.getDB(dbName).collectionExists(colName)

  //TODO -- these operations don't catch exceptions. if exceptions need to be catched here, then replace Boolean return values
  // with error statuses

  def createCollection(dbName: String, colName: String, spatialSpec: Option[SpatialSpec]) : Boolean = {
    if (! existsDb(dbName) || existsCollection(dbName, colName)) false
    else {
      //we give the capped option explicitly so that creation is not deferred!
      val created = mongo(dbName).createCollection(colName, MongoDBObject("capped" -> false))
      Logger.info("Created: " + created.getFullName)
      if (spatialSpec.isDefined) {
        Logger.info(created.getFullName + " is spatially enabled")
        val writeResult = mongo(dbName)(MetadataCollection).insert(MongoDBObject(
          ExtentField -> spatialSpec.get.envelope,
          IndexLevelField  -> spatialSpec.get.level,
          CollectionField -> colName
        ), WriteConcern.Safe)
        Logger.error(writeResult.getLastError().getErrorMessage)
      }
      true
    }
  }

  def getCollection(dbName: String, colName: String) : (Option[MongoCollection], Option[SpatialMetadata])  = {
    if (! existsCollection(dbName, colName))  (None, None)
    else {
      val col = mongo(dbName)(colName)
      val mdOpt = metadata(dbName, colName)
      val spatialMetadata = for( md <- mdOpt; smd <- md.spatialMetadata) yield smd
      (Some(col), spatialMetadata)
    }
  }

  def deleteCollection(dbName: String, colName: String) : Boolean = {
    if (! existsDb(dbName) || !existsCollection(dbName, colName) ) false
    else {
      Logger.info(s"Starting removal of $dbName/$colName")
      mongo(dbName)(colName).dropCollection()
      mongo(dbName)(MetadataCollection).remove(MongoDBObject( CollectionField -> colName), WriteConcern.Safe)
      Logger.info(s"Finalized removal of $dbName/$colName")
      true
    }
  }

  def query(database: String, collection: String, window: Envelope): Iterator[Feature] = {
    val md = metadata(database, collection)
    val coll = mongo(database)(collection)
    val src = MongoDbSource(coll, mkMortonContext(md.get.asInstanceOf[SpatialMetadata]))
    src.query(window)
  }

  private def mkMortonContext (md: SpatialMetadata): MortonContext = new MortonContext (md.envelope, md.level)

}

