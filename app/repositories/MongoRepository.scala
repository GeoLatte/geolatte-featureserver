package repositories


import org.geolatte.geom.curve.MortonContext
import org.geolatte.geom.Envelope
import com.mongodb.casbah.{MongoCollection, MongoClient}
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb.{SpatialCollectionMetadata, MetadataIdentifiers, MongoDbSource}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.DBObject

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/11/13
 */

//this needs to move to a service layer
object MongoRepository {


  import MetadataIdentifiers._

  //TODO -- make the client configurable
  val mongo = MongoClient()


  def listDatabases(): Traversable[String] = mongo.dbNames

  def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def listCollections(dbname: String): Option[Traversable[String]] =
    if (mongo.dbNames.exists(_ == dbname)) Some(mongo.getDB(dbname).collectionNames.filterNot(isMetadata(_)))
    else None

  def createDb(dbname: String): Boolean =
    if (mongo.dbNames.exists(_ == dbname)) false
    else {
      mongo(dbname).createCollection(MetadataCollection, MongoDBObject.empty)
      true
    }

  def deleteDb(dbname: String): Boolean =
    if (mongo.dbNames.exists(_ == dbname)) {
      mongo(dbname).dropDatabase
      true
    }
    else false

  def query(database: String, collection: String, window: Envelope): Iterator[Feature] = {
    val md = metadata(database, collection)
    val coll = mongo(database)(collection)
    val src = MongoDbSource(coll, mkMortonContext(md.get.asInstanceOf[SpatialMetadata]))
    src.query(window)

  }

  def metadata(database: String, collection: String): Option[Metadata] = {

    def toSpatialMetadata(dbobj: DBObject) =
      for {
        mongoMeta <- SpatialCollectionMetadata.from(dbobj)
        md = SpatialMetadata (envelope = mongoMeta.envelope, stats = mongoMeta.stats,
          name = mongoMeta.name, level = mongoMeta.level)
      } yield md

    import MetadataIdentifiers._
    mongo(database)(MetadataCollection).findOne(MongoDBObject(CollectionField -> collection)) match {
      case Some(md) => toSpatialMetadata(md)
      case None => {
        if (listCollections(database).exists( _ == collection)) None
        else Some(NonSpatialMetadata(collection))
      }
    }


  }

  private def mkMortonContext(md: SpatialMetadata): MortonContext = new MortonContext(md.envelope, md.level)


}
