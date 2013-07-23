package repositories


import org.geolatte.geom.curve.MortonContext
import org.geolatte.geom.Envelope
import com.mongodb.casbah.{MongoCollection, MongoClient}
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb.{MetadataIdentifiers, Metadata, MongoDbSource}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.DBObject
import play.api.cache.Cache
import play.api.Play.current
import collection.mutable

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/11/13
 */

//this needs to move to a service layer
object MongoRepository {

  //TODO -- make the client configurable
  val mongo = MongoClient()

  def listDatabases() : mutable.Buffer[String] = mongo.dbNames

  def query(database: String, collection: String, window: Envelope): Iterator[Feature] = {
    val md = metadata(database, collection)
    val coll = mongo(database)(collection)
    val src = MongoDbSource(coll, mkMortonContext(md))
    src.query(window)

  }

  def metadata(database: String, collection: String) : Metadata = {
    val key = s"${database}.${collection}"
    Cache.getOrElse[Metadata](key){
      val md= lookUpMetadata(database, collection)
      Cache.set(key, md)
      md
    }
  }

  def lookUpMetadata(database :String, collection: String) : Metadata = {
    import MetadataIdentifiers._
    val metadataCollection : MongoCollection = mongo(database)(MetadataCollection)
    metadataCollection.findOne( MongoDBObject( CollectionField -> collection)) match {
      case Some(obj) => toMetadata(obj)
      case _ => throw new NoSuchElementException(s"Can't find metadata for collection ${collection}")
    }
  }

  def toMetadata(obj: DBObject) : Metadata =
    Metadata.from(obj) match {
      case Some(md) => md
      case _ => throw new NoSuchElementException(s"Invalid metadata for collection")
    }

  def mkMortonContext(md: Metadata) : MortonContext = new MortonContext(md.envelope, md.level)


}
