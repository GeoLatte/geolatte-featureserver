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
import scala.language.implicitConversions

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

trait SpatialQuery[T] {
  def selector(col: MongoSpatialCollection) : JsObject
  def projection : JsObject
  def run(col : MongoSpatialCollection) : Future[T]
}

class BaseSpatialQuery (
      windowOpt: Option[Envelope],
      queryOpt: Option[JsObject],
      projectionOpt: Option[JsArray]) extends SpatialQuery[Enumerator[JsObject]] {

  //we require a MortonCodeQueryOptimizer to be mixed in on instantiation
  self: MortonCodeQueryOptimizer =>


  def window2query(window: Envelope, mortoncode: MortonCode) = {
    val qds: Seq[JsValue] = optimize(window, mortoncode)
    val docArr = JsArray(qds)
    Json.obj("$or" -> docArr)
  }

  def selector(col: MongoSpatialCollection) = {
    val windowPart = windowOpt.map(w => window2query(w,col.mortonCode)).getOrElse(Json.obj())
    val query = queryOpt.getOrElse(Json.obj())
    query ++ windowPart
  }

  def projection =  {
    val flds = projectionOpt.getOrElse( Json.arr() )
    //TODO can this be  simplified?
    Json.obj(flds.value.collect{case f : JsString => f.value -> Json.toJsFieldJsValueWrapper(1)}:_*)
  }

  def run(sc: MongoSpatialCollection) : Future[Enumerator[JsObject]] = {
    val cursor = sc.collection.find(selector(sc), projection).cursor[JsObject]
    Future{ filteringEnumeratee.map(fe => cursor.enumerate through fe).getOrElse(cursor.enumerate) }
  }

  // TODO this could probably be done more efficiently using a custom window-sensitive Json Validator
  private def filteringEnumeratee = {
    for (window <- windowOpt) yield {
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
}

object SpatialQuery {

  def apply() : SpatialQuery[Enumerator[JsObject]] =
    apply(None, None, None)

  def apply(window: Envelope) : SpatialQuery[Enumerator[JsObject]] =
    apply(Some(window), None, None)

  def apply(window: Envelope, query: JsObject) : SpatialQuery[Enumerator[JsObject]] =
    apply(Some(window), Some(query), None)

  def apply(window: Option[Envelope], query: Option[JsObject], projection: Option[JsArray]) : SpatialQuery[Enumerator[JsObject]] =
    new BaseSpatialQuery(window, query, projection)
      with SubdividingMCQueryOptimizer

  def withCount[T](base: SpatialQuery[T]) = new SpatialQuery[(Int, T)] {
        def selector(sc: MongoSpatialCollection) = base.selector(sc)
        def projection = base.projection
        def run(sc: MongoSpatialCollection) = {
          import BSONFormats.BSONDocumentFormat._
          val queryBson = partialReads(base.selector(sc)) match {
            case JsSuccess(qb, _) => qb
            case _ => throw new RuntimeException("Failure to convert JSON Query doc to BSONDocument")
          }
          val cntCmd = Count(sc.collection.name, Some(queryBson))
          sc.collection.db.command(cntCmd).flatMap( cnt => base.run(sc).map( e => (cnt, e)) )
        }
      }

  implicit def metadata2mortoncontext(metadata: Metadata) = new MortonContext(metadata.envelope, metadata.level)

}

case class MongoSpatialCollection(collection: JSONCollection, metadata: Metadata) {

  lazy val mortonContext = new MortonContext(metadata.envelope, metadata.level)
  def mortonCode = new MortonCode(mortonContext)

}





