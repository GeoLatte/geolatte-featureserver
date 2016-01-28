package nosql.mongodb

import org.geolatte.geom.Envelope
import querylang.BooleanExpr
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.DefaultBSONHandlers._

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.modules.reactivemongo._
import play.modules.reactivemongo.json.ImplicitBSONHandlers._

import collection.JavaConversions._
import org.geolatte.geom.curve.{MortonContext, MortonCode}

import play.Logger
import play.api.libs.iteratee._
import scala.util.{Success, Try, Failure}
import nosql.json.GeometryReaders._
import nosql.json.GeometryReaders

import scala.language.reflectiveCalls
import scala.language.implicitConversions

import config.AppExecutionContexts.streamContext
import nosql.{InvalidQueryException, SpatialQuery, Metadata, MortonCodeQueryOptimizer}
import play.api.libs.json.JsArray
import play.modules.reactivemongo.json.collection.JSONCollection


object SpecialMongoProperties {

  val MC = "_mc"
  val BBOX = "_bbox"
  val ID = "id"
  val _ID = "_id"

  val all = Set(MC, BBOX, ID, _ID)

  def isSpecialMongoProperty(key: String): Boolean = all.contains(key)

}

trait SubdividingMCQueryOptimizer extends MortonCodeQueryOptimizer {

  type QueryDocuments = List[JsObject]

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
    case e: Throwable => Failure(new InvalidQueryException(e.getMessage))
  }.get

}


abstract class MongoSpatialCollection(collection: JSONCollection, metadata: Metadata) {

  //we require a MortonCodeQueryOptimizer to be mixed in on instantiation
  this: SubdividingMCQueryOptimizer =>

  lazy val mortonContext = new MortonContext(metadata.envelope, metadata.level)

  def mortonCode = new MortonCode(mortonContext)


  private def render(expr : BooleanExpr) : JsObject = MongoDBQueryRenderer.render(expr) match {
    case jsvalue if jsvalue.isInstanceOf[JsObject] => jsvalue.asInstanceOf[JsObject]
    //TODO -- how better to handle exceptions
    case _ => throw new IllegalArgumentException();
  }

  def window2query(window: Envelope) = {
    val qds: Seq[JsValue] = optimize(window, mortonCode)
    val docArr = JsArray(qds)
    Json.obj("$or" -> docArr)
  }

  def selector(sq: SpatialQuery) = {
     val windowPart = sq.windowOpt.map( window2query(_) ).getOrElse(Json.obj())
     val query = sq.queryOpt.map( render(_) ).getOrElse(Json.obj())
     query ++ windowPart
  }

  def projection(sq: SpatialQuery) =  {
    //make sure we include  bbox and geometry property so that filtering works correctly
    if (sq.projection.isEmpty) Json.obj()
    else {
      val flds = sq.projection ++ List( SpecialMongoProperties.BBOX, "geometry", "type" )
      val fldPairs = flds.map { f => (f -> Json.toJsFieldJsValueWrapper( 1 )) }
      Json.obj( fldPairs: _* )
    }
  }

  def run(query: SpatialQuery) : Enumerator[JsObject] = {
    val sel = selector(query)
    val proj = projection(query)
    Logger.debug(s"Run query with selector: ${Json.stringify(sel)}; and projection: ${Json.stringify(proj)} ")
    val cursor = collection.find(sel, proj).cursor[JsObject]
    query.windowOpt match {
      case Some(w) => cursor.enumerate() through filteringEnumeratee(w)
      case _ => cursor.enumerate()
    }
  }

  // TODO this could probably be done more efficiently using a custom window-sensitive Json Validator
  private def filteringEnumeratee(window: Envelope) = {
      import GeometryReaders.extentFormats
      val toExtent = Enumeratee.map[JsObject](obj => (obj, (obj \ SpecialMongoProperties.BBOX).asOpt[Extent]))
      val filter = Enumeratee.filter[(JsObject, Option[Extent])](p => p match {
        case (obj, None) => false
        case (_, Some(ex)) => window.intersects(ex.toEnvelope(window.getCrsId))
      })
      val toObj = Enumeratee.map[(JsObject, Option[Extent])](p => p._1)
      (toExtent compose filter compose toObj)
    }

}

object MongoSpatialCollection {

  def apply(collection: JSONCollection, metadata: Metadata) = new MongoSpatialCollection(collection, metadata)
    with SubdividingMCQueryOptimizer

}





