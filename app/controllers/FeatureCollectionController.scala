package controllers

import scala.language.reflectiveCalls
import scala.language.implicitConversions

import org.geolatte.geom.{Point, Geometry, Envelope}
import org.geolatte.geom.crs.CrsId
import play.api.mvc._
import play.api.Logger
import nosql.mongodb._
import play.api.libs.iteratee._
import config.AppExecutionContexts
import play.api.libs.json._
import nosql.json.GeometryReaders._
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import scala.concurrent.Future
import play.api.Play._

import play.api.libs.json.JsString
import play.api.libs.json.JsBoolean

import play.api.libs.json.JsNumber
import utilities.{EnumeratorUtility, QueryParam}
import nosql.Exceptions.InvalidQueryException
import play.api.libs.json.JsObject
import com.fasterxml.jackson.core.JsonParseException
import nosql.{Metadata, SpatialQuery, FutureInstrumented}



object FeatureCollectionController extends AbstractNoSqlController with FutureInstrumented {

  import AppExecutionContexts.streamContext

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))

    val WITH_VIEW = QueryParam("with-view", (s: String) => Some(s))

    val LIMIT = QueryParam("limit", (s:String) => Some(s.toInt))

    val START = QueryParam("start", (s:String) => Some(s.toInt))

    val PROJECTION : QueryParam[JsArray] = QueryParam("projection", (s:String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty PROJECTION parameter")
      else Some(JsArray( s.split(',').toSeq.map(e => JsString(e)) ))
    )

    val QUERY : QueryParam[JsObject] = QueryParam("query", (s:String) =>
      try {
        Json.parse(s).asOpt[JsObject]
      } catch {
        case e : JsonParseException => throw  InvalidQueryException(e.getMessage)
      })

  }

  val COLLECTION_LIMIT = current.configuration.getInt("max-collection-size").getOrElse[Int](10000)

  def query(db: String, collection: String) =
    repositoryAction ( implicit request => futureTimed("featurecollection-query"){
        implicit val queryStr = request.queryString
        Logger.info(s"Query string $queryStr on $db, collection $collection")
        repository.metadata(db, collection).flatMap(md =>
          doQuery(db, collection, md).map[SimpleResult](x => x)
        ).recover {
          case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
        }.recover (commonExceptionHandler(db, collection))
    })


  def download(db: String, collection: String) = repositoryAction {
      implicit request => {
        Logger.info(s"Downloading $db/$collection.")
        repository.query(db, collection, SpatialQuery()).map[SimpleResult](x => x).recover {
          commonExceptionHandler(db, collection)
        }
      }
    }


  def collectFeatures(start: Int, max: Int) : Iteratee[JsObject, (Int, List[JsObject])] = {

    case class fState(features: List[JsObject] = Nil, collected: Int = 0, total: Int = 0)

    Iteratee.fold[JsObject, fState]( fState( total = start ) ) ( (state, feature) =>
      if (state.collected >= max) state.copy( total = state.total+1)
      else fState(feature::state.features, state.collected + 1, state.total + 1)
    ).map(state => (state.total, state.features))

  }

  def list(db: String, collection: String) = repositoryAction(
    implicit request => futureTimed("featurecollection-list"){
      implicit val queryStr = request.queryString
      def enumerator2list(enum: Enumerator[JsObject]) : Future[(Int, List[JsObject])] = {
        val limit = Math.min( QueryParams.LIMIT.extract.getOrElse(COLLECTION_LIMIT),COLLECTION_LIMIT)
        val start = QueryParams.START.extract.getOrElse(0)
        enum &> Enumeratee.drop(start)  |>>> collectFeatures(start, limit)
      }
      Logger.info(s"Query string $queryStr on $db, collection $collection")
      repository.metadata(db, collection).flatMap(md =>
        doQuery(db, collection, md).flatMap {
          case enum => enumerator2list(enum)
        }.map[SimpleResult]{
          case (cnt, features) => toSimpleResult(FeaturesResource(cnt, features))
        }
      ).recover {
        case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
      }.recover (commonExceptionHandler(db, collection))

    }
  )

  /**
   * converts a JsObject Enumerator to an RenderableStreamingResource supporting both Json and Csv output
   *
   * Limitations: when the passed Json is not a valid GeoJson object, this will pass a stream of empty points
   * @param enum
   * @param req
   * @return
   */
  implicit def enumJsontoResult(enum: Enumerator[JsObject])(implicit req: RequestHeader): SimpleResult =
    toSimpleResult(new JsonStreamable with CsvStreamable {

      val encoder = new DefaultCsvEncoder()

      def encode(v: JsString) = "\"" + encoder.encode(v.value, null, CsvPreference.STANDARD_PREFERENCE) + "\""


      def expand(v : JsObject) : Seq[(String, String)] = utilities.JsonHelper.flatten(v).map {
        case (k, v: JsString)   => (k, encode(v) )
        case (k, v: JsNumber)   => (k, Json.stringify(v) )
        case (k, v: JsBoolean)  => (k, Json.stringify(v) )
        case (k, _) => (k, "")
      }


      def project(js: JsObject)(selector: PartialFunction[(String, String), String], geomToString: Geometry => String) : Seq[String] = {
        val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
        val attributes = expand(jsObj).collect(selector)
        val geom = geomToString((js \ "geometry").asOpt(GeometryReads(CrsId.UNDEFINED)).getOrElse(Point.createEmpty()))
        val idOpt = (js \ "_id" \ "$oid").asOpt[String].map(v => ("_id", v)).getOrElse(("_id", "null"))
        selector(idOpt) +: geom +: attributes
      }

      val toCsvRecord = (js: JsObject) => project(js)({
        case (k, v) => v
        case _ => "None"
      }, g => g.asText).mkString(",")

      val toCsvHeader = (js: JsObject) => project(js)({
        case (k, v) => k
        case _ => "None"
      }, _ => "geometry-wkt").mkString(",")

      val toCsv : (Int, JsObject) => String = (i,js) => {
        if (i != 0) toCsvRecord(js)
        else toCsvHeader(js)  + "\n" + toCsvRecord(js)
      }

      def toJsonStream = enum

      def toCsvStream = EnumeratorUtility.withIndex(enum).map[String]( toCsv.tupled)

    })

  private def doQuery(db: String, collection: String, smd: Metadata)
                            (implicit queryStr: Map[String, Seq[String]]) : Future[Enumerator[JsObject]] =
    queryString2SpatialQuery(db,collection,smd).flatMap( q =>  repository.query(db, collection, q) )


  implicit def queryString2SpatialQuery(db: String, collection: String, smd: Metadata)
                                       (implicit queryStr: Map[String, Seq[String]]) : Future[SpatialQuery] = {
    val windowOpt = Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId)
    val projectionOpt = QueryParams.PROJECTION.extract
    val queryParamOpt = QueryParams.QUERY.extract
    val viewDef = QueryParams.WITH_VIEW.extract.map(vd => repository.getView(db, collection, vd))
      .getOrElse(Future {
      Json.obj()
    })
    viewDef.map(vd => vd.as(Formats.ViewDefExtract))
      .map {
      case (queryOpt, projOpt) =>
        SpatialQuery(
          windowOpt,
          jsOptMerge(queryOpt, queryParamOpt)(_ ++ _),
          jsOptMerge(projOpt, projectionOpt)(_ ++ _))
    }
  }

  private def jsOptMerge[J <: JsValue](elem: Option[J]*)(union: (J, J) => J): Option[J] =
    elem.foldLeft(None: Option[J]) {
      (s, e) => e match {
        case None => s
        case Some(_) if s.isDefined => e.map(js => union(s.get, js))
        case _ => e
      }
    }

  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try {
            val env = new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs)
            if (!env.isEmpty) Some(env)
            else None
          } catch {
            case _: Throwable => None
          }
        }
        case _ => None
      }
    }
  }


}