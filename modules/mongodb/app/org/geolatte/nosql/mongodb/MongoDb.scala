package org.geolatte.nosql.mongodb

import org.geolatte.common.Feature
import collection.JavaConversions._
import org.geolatte.geom.codec.Wkb
import org.geolatte.geom.{Geometry, Envelope, ByteBuffer, ByteOrder}
import org.geolatte.geom.curve.{MortonContext, MortonCode}
import org.geolatte.nosql.{WindowQueryable, Source, Sink}

import java.util
import play.Logger
import sun.misc.{BASE64Decoder, BASE64Encoder}
import org.geolatte.scala.ChainedIterator
import reactivemongo.api.collections.default.BSONCollection
import scala.concurrent.{ExecutionContext, Future}
import reactivemongo.bson._
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import reactivemongo.core.commands.{LastError, GetLastError}
import reactivemongo.api.Cursor
import play.api.libs.iteratee._
import reactivemongo.api.indexes.Index
import scala.Some
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.core.commands.GetLastError
import reactivemongo.bson.BSONString
import scala.util.{Failure, Success}


//TODO == replace with Apache commons-codec (sun.misc.* classes shouldnot be called directly)


//TODO get the right execution context
import ExecutionContext.Implicits.global;

object MetadataIdentifiers {
  val MetadataCollectionPrefix = "geolatte_nosql."
  val MetadataCollection = "geolatte_nosql.collections"
  val ExtentField = "extent"
  val IndexLevelField = "index_depth"
  val CollectionField = "collection"
  val Fields = Set(ExtentField, CollectionField)
}

object SpecialMongoProperties {

  val WKB = "_wkb"
  val MC = "_mc"
  val BBOX = "_bbox"
  val ID = "id"
  val _ID = "_id"

  val all = Set(WKB, MC, BBOX, ID, _ID)

  def isSpecialMongoProperty(key: String): Boolean = all.contains(key)

}

trait FeatureWithEnvelope extends Feature {
  def envelope: Envelope
}

class MongoDbSink(val collection: BSONCollection, val mortoncontext: MortonContext) extends Sink[Feature] {

  import scala.collection.mutable.Map

  private val GROUP_SIZE = 5000
  val mortoncode = new MortonCode(mortoncontext)

  private def beforeIn = {
    val idxManager = collection.indexesManager
    idxManager.list().flatMap(indexes => Future.sequence(indexes.map(idxManager.delete(_))))
  }

  def in(features: Enumerator[Feature])  = {

      def transform(f: Feature): Option[BSONDocument] = {
        try {
          val mc = mortoncode ofGeometry f.getGeometry
          Some(MongoDbFeature(f, mc))
        } catch {
          case ex: IllegalArgumentException => {
            Logger.warn(" Can't save feature with envelope " + f.getGeometry.getEnvelope.toString)
            None
          }
        }
      }

      val convertingToDoc = Enumeratee.map[Feature]( f => transform(f) )
      val filterOutNoneVals = Enumeratee.collect[Option[BSONDocument]]{ case Some(doc) => doc }
      val bulkInsert = collection.bulkInsertIteratee(bulkSize = GROUP_SIZE)
      features apply ((convertingToDoc compose filterOutNoneVals) transform bulkInsert)

      afterIn.onComplete{
        case Success(le) if le.ok => Logger.info("Successfully rebuilt index and updated properties")
        case Failure(t) => Logger.info("Failure on reindex", t)
        case Success(le) => Logger.info("Failed to rebuild index. Message is " + le.errMsg)
      }

    }

  private def afterIn = {

    val idxManager = collection.indexesManager
    val futureComplete = idxManager.create( new Index(Seq((SpecialMongoProperties.MC, Ascending))))

    futureComplete.onFailure {
      case ex => Logger.warn("Failure on creating mortoncode index on collection %s" format collection.name)
    }

    import MetadataIdentifiers._

    val metadata = BSONDocument(
      CollectionField -> collection.name,
      ExtentField -> EnvelopeSerializer(mortoncontext.getExtent),
      IndexLevelField -> mortoncontext.getDepth

    )
    val selector = BSONDocument("collection" -> collection.name)
    val mdCollection = collection.db.collection[BSONCollection](MetadataIdentifiers.MetadataCollection)
    //TODO set getlasterror as defined values in ConfigurationValues object
    mdCollection.update(selector = selector, update = metadata, writeConcern = new GetLastError(true, None, true),  upsert = true, multi = false )
  }

}


trait MortonCodeQueryOptimizer {
  //the return type of the optimizer
  type QueryDocuments = List[BSONDocument]

  /**
   * Optimizes the window query, given the specified MortonCode
   * @param window
   * @return
   */
  def optimize(window: Envelope, mortoncode: MortonCode): QueryDocuments
}

trait SubdividingMCQueryOptimizer extends MortonCodeQueryOptimizer {

  def optimize(window: Envelope, mortoncode: MortonCode): QueryDocuments = {

    /*
    recursively divide the subquery to lowest level
     */
    def divide(mc: String): Set[String] = {
      if (!(window intersects (mortoncode envelopeOf mc))) Set()
      else if (mc.length == mortoncode.getMaxLength) Set(mc)
      else divide(mc + "0") ++ divide(mc + "1") ++ divide(mc + "2") ++ divide(mc + "3")
    }

    /*
    expand a set of mortoncodes to all mortoncodes of predecessors. i.e expand "00" to
    "", "0","00"
     */
    def expand(mcs: Set[String]): Set[String] = {
      Set("") ++ (for (mc <- mcs; i <- 0 to mc.length) yield mc.substring(0, i)).toSet
    }

    //maps the set of mortoncode strings to a list of querydocuments
    def toQueryDocuments(mcSet: Set[String]): QueryDocuments = {
      mcSet.map(mc => BSONDocument(SpecialMongoProperties.MC -> mc)).toList
    }

    val mc = mortoncode ofEnvelope window
    val result = (divide _ andThen expand _ andThen toQueryDocuments _)(mc)
    Logger.debug(s"num. of queries for window ${window.toString}= ${result.size}")
    result
  }

}


class MongoDbSource(val collection: BSONCollection, val mortoncontext: MortonContext)
  extends Source[Feature]
  with WindowQueryable[Feature] {

  //we require a MortonCodeQueryOptimizer to be mixed in on instantiation
  this: MortonCodeQueryOptimizer =>

  val mortoncode = new MortonCode(mortoncontext)

  //is there no way to get around the ugly typecast? Enumeratee methods aren't covariant (so that is now the problem)
  def out(): Enumerator[Feature] = toFeatureIterator(collection.find(BSONDocument()).cursor) &> Enumeratee.map(f => f.asInstanceOf[Feature])

  /**
   *
   * @param window the query window
   * @return
   * @throws IllegalArgumentException if Envelope does not fall within context of the mortoncode
   */
  def query(window: Envelope): Enumerator[Feature] = {
    val mcQueryWindow = mortoncode ofEnvelope window
    val qds : Traversable[BSONValue] = optimize(window, mortoncode)
    val docArr = BSONArray(qds)
    val query = BSONDocument("$or" -> docArr)
    toFeatureIterator(collection.find(query).cursor) &> Enumeratee.collect{ case f if window.intersects(f.envelope) => f }
  }

  private def toFeatureIterator(cursor: Cursor[BSONDocument]): Enumerator[FeatureWithEnvelope] = {
    val toFeature: Enumeratee[BSONDocument, Option[FeatureWithEnvelope]] = Enumeratee.map[BSONDocument]{ doc => MongoDbFeature.toFeature(doc) }
    val optFilter: Enumeratee[Option[FeatureWithEnvelope], FeatureWithEnvelope] = Enumeratee.collect{ case Some(f) => f}
    cursor.enumerate through toFeature through optFilter
  }

}

object MongoDbSource {

  def apply(collection: BSONCollection, mortoncontext: MortonContext): MongoDbSource =
    new MongoDbSource(collection, mortoncontext) with SubdividingMCQueryOptimizer

}

/**
 * Converts between features and DBObjects
 *
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/2/13
 */
object MongoDbFeature {

  def propertyMap(feature: Feature): Seq[(String, Any)] = {
    feature.getProperties.map(p => (p, feature.getProperty(p))).toSeq
  }

  def geometryProperties(feature: Feature, mortoncode: String): Seq[(String, String)] = {
    val geom = feature.getGeometry
    val geometryEncoder = Wkb.newEncoder()
    val base64Encoder = new BASE64Encoder()
    val wkbBASE64 = base64Encoder.encode(geometryEncoder.encode(geom, ByteOrder.XDR).toByteArray)
    val bbox = EnvelopeSerializer(geom.getEnvelope)
    import SpecialMongoProperties._
    (WKB, wkbBASE64) ::(MC, mortoncode) ::(BBOX, bbox) :: Nil
  }

  def apply(feature: Feature, mortoncode: String) = {
    import DefaultBSONHandlers._
    val elems = propertyMap(feature) ++ geometryProperties(feature, mortoncode)
    BSONDocument( elems.map{
      case (k, v: String) => (k, BSONString(v))
      case (k, v: Int) => (k, BSONInteger(v))
      case (k, v: Double) => (k, BSONDouble(v))
      //TODO -- there should be  much better solution than this!!
    } )
  }

  def toFeature(obj: BSONDocument): Option[DBObjectFeature] = {
    if (obj.get(SpecialMongoProperties.WKB) == None || obj.get(SpecialMongoProperties.BBOX) == None) return None
    Some(new DBObjectFeature(obj))
  }

  /**
   * A Feature implementation that adapts a MongoDBObject
   *
   * @param obj the adapted MongoDBObject
   */
  class DBObjectFeature(val obj: BSONDocument) extends FeatureWithEnvelope {

    import SpecialMongoProperties._

    lazy private val geometry: Geometry = {
      val base64Decoder = new BASE64Decoder()
      val geometryDecoder = Wkb.newDecoder()
      val wkbStr = obj.getAs[String](WKB).get //TODO ==> this fails if there is no WKB property
      val bytes = base64Decoder.decodeBuffer(wkbStr)
      geometryDecoder.decode(ByteBuffer.from(bytes))
    }


    lazy private val propertyMap = obj.elements.filter { case (k,v) => !isSpecialMongoProperty(k) }.toMap

    //TODO -- dangerouse get on bbox option + convoluted
    val envelope = EnvelopeSerializer.unapply(obj.getAs[String](BBOX).get).getOrElse(geometry.getEnvelope)

    def hasProperty(propertyName: String, inclSpecial: Boolean): Boolean =
      (inclSpecial && ("id".equals(propertyName) || "geometry".equals(propertyName))) ||
        (propertyMap.keys.contains(propertyName))

    def getProperties: util.Collection[String] = propertyMap.keys

    def getProperty(propertyName: String): AnyRef =
      if (propertyName == null) throw new IllegalArgumentException("Null parameter passed.")
      else propertyMap.get(propertyName).getOrElse(null) //when None, we return null to conform to the interface definition

    def getId: AnyRef = obj.get(ID).getOrElse("no id")

    def getGeometry: Geometry = geometry

    def getGeometryName: String = "geometry"

    def hasId: Boolean = true

    def hasGeometry: Boolean = true

  }

}




