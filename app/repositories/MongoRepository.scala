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


  import MetadataIdentifiers._

  //TODO -- make the client configurable
  val mongo = MongoClient()



  def listDatabases() : mutable.Buffer[String] = mongo.dbNames

  def isMetadata(name : String) : Boolean =  (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  def listCollections(dbname : String) =
  if ( mongo.dbNames.exists( _ == dbname ) ) Some( mongo.getDB(dbname).collectionNames.filterNot( isMetadata(_)) )
  else None

  def createDb(dbname : String) : Boolean =
  if(mongo.dbNames.exists( _ == dbname )) false
  else {
    mongo(dbname).createCollection(MetadataCollection, MongoDBObject.empty)
    true
  }

  def deleteDb(dbname: String): Boolean =
    if (mongo.dbNames.exists( _ == dbname)) {
      mongo(dbname).dropDatabase
      true
    }
    else false

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
