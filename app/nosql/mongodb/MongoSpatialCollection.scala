package nosql.mongodb

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.DefaultBSONHandlers._

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.modules.reactivemongo._
import play.modules.reactivemongo.json.ImplicitBSONHandlers._

import collection.JavaConversions._
import org.geolatte.geom.codec.Wkb
import org.geolatte.geom.{Geometry, Envelope, ByteBuffer, ByteOrder}
import org.geolatte.geom.curve.{MortonContext, MortonCode}

import play.Logger
import scala.concurrent.{ExecutionContext, Future}
import reactivemongo.api.indexes.IndexType.Ascending
import reactivemongo.api.Cursor
import play.api.libs.iteratee._
import reactivemongo.api.indexes.Index
import scala.Some
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.core.commands.{Count, GetLastError}
import scala.util.{Try, Failure, Success}
import java.util.Date
import play.modules.reactivemongo.json.collection.JSONCollection
import nosql.json.GeometryReaders._
import nosql.json.GeometryReaders

import scala.language.reflectiveCalls

import config.AppExecutionContexts.streamContext
import nosql.Exceptions
import play.modules.reactivemongo.json.collection.JSONCollection
import scala.util.Failure
import scala.Some
import play.modules.reactivemongo.json.BSONFormats
import play.modules.reactivemongo.json.collection.JSONCollection
import scala.util.Failure
import scala.Some
import play.api.libs.json.JsArray
import play.modules.reactivemongo.json.collection.JSONCollection
import scala.util.Failure
import scala.Some


object MetadataIdentifiers {
  val MetadataCollectionPrefix = "geolatte_nosql."
  val MetadataCollection = "geolatte_nosql.collections"
  val ExtentField = "extent"
  val IndexLevelField = "index_depth"
  val CollectionField = "collection"
  val Fields = Set(ExtentField, CollectionField)
}

object SpecialMongoProperties {

  val MC = "_mc"
  val BBOX = "_bbox"
  val ID = "id"
  val _ID = "_id"

  val all = Set(MC, BBOX, ID, _ID)

  def isSpecialMongoProperty(key: String): Boolean = all.contains(key)

}

trait MortonCodeQueryOptimizer {
  //the return type of the optimizer
  type QueryDocuments = List[JsObject]

  /**
   * Optimizes the window query, given the specified MortonCode
   * @param window
   * @return
   */
  def optimize(window: Envelope, mortoncode: MortonCode): QueryDocuments
}

trait SubdividingMCQueryOptimizer extends MortonCodeQueryOptimizer {

  def optimize(window: Envelope, mortoncode: MortonCode): QueryDocuments = Try{

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
      mcSet.map(mc => Json.obj(SpecialMongoProperties.MC -> mc)).toList
    }

    val mc = mortoncode ofEnvelope window
    val result = (divide _ andThen expand _ andThen toQueryDocuments _)(mc)
    Logger.debug(s"num. of queries for window ${window.toString}= ${result.size}")
    result
  }.recoverWith {
    case e: Throwable => Failure(new Exceptions.InvalidQueryException(e.getMessage))
  }.get

}

case class Metadata(name: String, envelope: Envelope, level : Int, count: Long = 0)

object Metadata {

  import MetadataIdentifiers._

  //added so that MetadataReads compiles
  def apply(name: String, envelope:Envelope, level: Int): Metadata = this(name, envelope,level, 0)

  implicit val MetadataReads = (
    (__ \ CollectionField).read[String] and
    (__ \ ExtentField).read[Envelope](EnvelopeFormats) and
    (__ \ IndexLevelField).read[Int]
  )(Metadata.apply _)

}

class MongoSpatialCollection(val collection: JSONCollection, val mortoncontext: MortonContext) {

  //we require a MortonCodeQueryOptimizer to be mixed in on instantiation
  this: MortonCodeQueryOptimizer =>

  val mortoncode = new MortonCode(mortoncontext)


  def out(): Enumerator[JsObject] = collection.find(Json.obj()).cursor[JsObject].enumerate

  private def window2query(window: Envelope)= {
    val qds : Seq[JsValue] = optimize(window, mortoncode)
    val docArr = JsArray(qds)
    Json.obj("$or" -> docArr)
  }
  /**
   *
   * @param window the query window
   * @return
   * @throws IllegalArgumentException if Envelope does not fall within context of the mortoncode
   */
  def query(window: Envelope): Enumerator[JsObject] = {
    val query = window2query(window)
    filteringEnumerator(collection.find(query).cursor, window)
  }

  def queryWithCnt(window: Envelope): Future[(Int, Enumerator[JsObject])] = {
      val query = window2query(window)
      import BSONFormats.BSONDocumentFormat._
      val queryBson = partialReads(query) match {
        case JsSuccess(qb, _) => qb
        case _ => throw new RuntimeException("Failure to convert JSON Query doc to BSONDocument")
      }
      val cntCmd = Count(collection.name, Some(queryBson))
      collection.db.command(cntCmd).map( cnt => (cnt, filteringEnumerator(collection.find(query).cursor, window)))
    }



  // TODO this could probably be done more efficiently using a custom window-sensitive Json Validator
  private def filteringEnumerator(cursor: Cursor[JsObject], window : Envelope): Enumerator[JsObject] = {
    import GeometryReaders.extentFormats
    val toExtent  = Enumeratee.map[JsObject]( obj => (obj, (obj \  SpecialMongoProperties.BBOX).asOpt[Extent]))
    val filter = Enumeratee.filter[(JsObject, Option[Extent])]( p => p match {
        case (obj , None) => false
        case ( _ , Some(ex)) => window.contains(ex.toEnvelope(window.getCrsId))
        } )
    val toObj = Enumeratee.map[(JsObject, Option[Extent])]( p => p._1)
    cursor.enumerate through (toExtent compose filter compose toObj)
  }

}

object MongoSpatialCollection {

  def apply(collection: JSONCollection, metadata: Metadata): MongoSpatialCollection =
    new MongoSpatialCollection(collection, new MortonContext(metadata.envelope, metadata.level)) with SubdividingMCQueryOptimizer

}




