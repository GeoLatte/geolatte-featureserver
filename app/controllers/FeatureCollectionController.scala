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
import play.api.libs.json.JsUndefined
import play.api.libs.json.JsString
import play.api.libs.json.JsBoolean
import scala.Some
import play.api.libs.json.JsNumber
import utilities.QueryParam
import nosql.Exceptions.InvalidQueryException
import play.api.libs.json.JsObject
import play.api.libs.iteratee


object FeatureCollectionController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))
    val WITH_VIEW = QueryParam("with-view", (s: String) => Some(s))
    val LIMIT = QueryParam("limit", (s:String) => Some(s.toInt))
    val START = QueryParam("start", (s:String) => Some(s.toInt))
  }

  val COLLECTION_LIMIT = current.configuration.getInt("max-collection-size").getOrElse[Int](10000)

  def query(db: String, collection: String) = repositoryAction ( repo =>
    implicit request => {
        implicit val queryStr = request.queryString
        Logger.info(s"Query string $queryStr on $db, collection $collection")
        repo.metadata(db, collection).flatMap(md =>
          qetQueryResult(db, collection, md).map[Result](x => x)
        ).recover {
          case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
        }.recover (commonExceptionHandler(db, collection))
    })

  def download(db: String, collection: String) = repositoryAction { repo =>
    implicit request => {
        Logger.info(s"Downloading $db/$collection.")
        repo.query(db, collection, SpatialQuery()).map[Result](x => x).recover {
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

  def list(db: String, collection: String) = repositoryAction( repo =>
    implicit request => {
      implicit val queryStr = request.queryString
      def enumerator2list(enum: Enumerator[JsObject]) : Future[(Int, List[JsObject])] = {
        val limit = Math.min( QueryParams.LIMIT.extract.getOrElse(COLLECTION_LIMIT),COLLECTION_LIMIT)
        val start = QueryParams.START.extract.getOrElse(0)
        enum &> Enumeratee.drop(start)  |>>> collectFeatures(start, limit)
      }

      Logger.info(s"Query string $queryStr on $db, collection $collection")
      repo.metadata(db, collection).flatMap(md =>
        qetQueryResult(db, collection, md).flatMap {
          case enum => enumerator2list(enum)
        }.map[Result]{
          case (cnt, features) => toResult(FeaturesResource(cnt, features))
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
  implicit def enumJsontoResult (enum : Enumerator[JsObject])(implicit req : RequestHeader) : Result =
    toResult(new JsonStreamable with CsvStreamable {

        val encoder = new DefaultCsvEncoder()

        def encode(v: JsString) = "\"" + encoder.encode(v.value, null, CsvPreference.STANDARD_PREFERENCE) + "\""

        def project(js: JsObject)(pf: PartialFunction[Option[(String, String)], String], pf2: Geometry => String) = {
          val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
          val attributes = jsObj.fields.map{
              case (k,v: JsString) => Some((k, encode(v)))
              case (k,v: JsNumber) => Some((k,v.value.toString))
              case (k,v: JsBoolean) => Some((k,v.value.toString))
              case (k,JsNull) => Some((k, ""))
              case (k, _ : JsUndefined) => Some((k, ""))
              case _ => None
            }.collect(pf)
          val geom = pf2((js \ "geometry").asOpt(GeometryReads(CrsId.UNDEFINED)).getOrElse(Point.createEmpty()))
          geom +: attributes
        }

        val toCsvRecord = (js: JsObject) => project(js)({case Some((k,v)) => v }, g => g.asText).mkString(",")
        val toCsvHeader = (js:JsObject) => project(js)({case Some((k,v)) => k },  _ => "geometry-wkt").mkString(",")

        // toCsv works as a state machine, on first invocation it print header, and record,
        // on subsequent invocations, only the record
        var toCsv : (JsObject) => String = js => {
          this.toCsv = toCsvRecord
          toCsvHeader(js) + "\n" + toCsvRecord(js)
        }

        def toJsonStream = enum
        def toCsvStream = enum.map(js => toCsv(js))

      }
  )

  private def qetQueryResult(db: String, collection: String, smd: Metadata)
                            (implicit queryStr: Map[String, Seq[String]]) : Future[Enumerator[JsObject]] =
    queryString2SpatialQuery(db,collection,smd).flatMap( q =>  MongoRepository.query(db, collection, q) )


  implicit def queryString2SpatialQuery(db: String, collection: String, smd: Metadata)
                                       (implicit queryStr: Map[String, Seq[String]]) = {
    val windowOpt = Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId)
    val viewDef = QueryParams.WITH_VIEW.extract.map(vd => MongoRepository.getView(db, collection, vd))
      .getOrElse(Future {Json.obj() })
    viewDef.map(vd => SpatialQuery(windowOpt, (vd \ "query").asOpt[JsObject], (vd \ "projection").asOpt[JsArray]))
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