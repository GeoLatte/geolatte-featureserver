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
import reactivemongo.core.commands.GetLastError
import scala.util.{Try, Failure, Success}
import java.util.Date
import play.modules.reactivemongo.json.collection.JSONCollection
import nosql.json.GeometryReaders.Extent
import nosql.json.GeometryReaders

import scala.language.reflectiveCalls

import config.AppExecutionContexts.streamContext
import nosql.Exceptions
;

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
  } match {
    case Success(v) => v
    case Failure(e) => throw new Exceptions.InvalidQueryException(e.getMessage)
  }

}


class MongoDbSource(val collection: JSONCollection, val mortoncontext: MortonContext)
  extends Source[JsObject] {

  //we require a MortonCodeQueryOptimizer to be mixed in on instantiation
  this: MortonCodeQueryOptimizer =>

  val mortoncode = new MortonCode(mortoncontext)


  def out(): Enumerator[JsObject] = collection.find(JsNull).cursor[JsObject].enumerate

  /**
   *
   * @param window the query window
   * @return
   * @throws IllegalArgumentException if Envelope does not fall within context of the mortoncode
   */
  def query(window: Envelope): Enumerator[JsObject] = {
    val qds : Seq[JsValue] = optimize(window, mortoncode)
    val docArr = JsArray(qds)
    val query = Json.obj("$or" -> docArr)
    filteringEnumerator(collection.find(query).cursor, window)
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

object MongoDbSource {

  def apply(collection: JSONCollection, mortoncontext: MortonContext): MongoDbSource =
    new MongoDbSource(collection, mortoncontext) with SubdividingMCQueryOptimizer

}




