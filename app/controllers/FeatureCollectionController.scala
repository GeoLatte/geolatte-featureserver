package controllers

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}
import play.api.Logger
import util.{MediaTypeSpec, QueryParam}
import play.api.libs.iteratee._
import org.geolatte.common.Feature
import repositories.MongoRepository
import org.geolatte.nosql.mongodb.SpatialMetadata
import config.ConfigurationValues.{Version, Format}
import config.{ConfigurationValues, AppExecutionContexts}
import scala.concurrent.Future
import scala.Some
import util.QueryParam


object FeatureCollection extends Controller {

  //TODO -- check choice of execution context

  import AppExecutionContexts.streamContext

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))
  }

  //temporary fixed value
  val WGS_84 = CrsId.valueOf(4326)

  def query(db: String, collection: String) = Action {
    request =>
      implicit val queryStr = request.queryString
      Logger.info(s"Query string ${queryStr} on $db, collection $collection")
      Async {
        doQuery(db, collection)
      }

  }

  def download(db: String, collection: String) = Action{
    request =>
      Async {
        Logger.info(s"Downloading $db/$collection.")

        MongoRepository.existsCollection(db, collection).flatMap {
          case false => Future.successful(NotFound("s${db}/${collection} not found"))
          case true => MongoRepository.getData(db, collection) map ( fs => mkChunked(fs))
        }
      }

  }

  def doQuery(db: String, collection: String)(implicit queryStr: Map[String, Seq[String]])  = {
      val fmeta = MongoRepository.metadata(db, collection)
      fmeta.flatMap ( f => f.spatialMetadata match {
              case Some(smd: SpatialMetadata) => qetQueryResult(db, collection, smd)
              case None => Future.successful(BadRequest(s"BadRequest: Not a spatial collection."))
      })
  }

  private def qetQueryResult(db: String, collection: String, smd: SpatialMetadata)(implicit queryStr: Map[String, Seq[String]]) = {
    Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId) match {
      case Some(window) =>
        MongoRepository.query(db, collection, window) map (q => mkChunked(q))
      case None => Future.successful(BadRequest(s"BadRequest: No or invalid bbox parameter in query string."))
    }
  }

  def mkChunked(features : Enumerator[Feature]) = {
      Ok.stream( toStream(features) andThen Enumerator.eof ).as(MediaTypeSpec(Format.JSON, Version.default))
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

  def toStream(features: Enumerator[Feature], chunkSize: Int = 1024 * 8) : Enumerator[Array[Byte]] = {
    val jsonMapper = new JsonMapper()

    //this is due to James Roper (see https://groups.google.com/forum/#!topic/play-framework/PrPTIrLdPmY)
    class CommaSeparate extends Enumeratee.CheckDone[String, String] {
      def continue[A](k: K[String, A]) = Cont {
        case in @ (Input.Empty) => this &> k(in)
        case in: Input.El[String] => Enumeratee.map[String](ConfigurationValues.jsonSeparator + _) &> k(in)
        case Input.EOF => Done(Cont(k), Input.EOF)
      }
    }

    def toString(f: Feature) = try {
      jsonMapper.toJson(f)
    } catch {
      case ex: Throwable =>
        Logger.error("Failure: " + ex.getMessage)
        "null"
    }

    val commaSeparate = new CommaSeparate
    val jsons = features.map( toString(_) ) &> commaSeparate

    val toBytes = Enumeratee.map[String]( _.getBytes("UTF-8") )
    //TODO Can we find a way to count the number of elements?
    jsons &> toBytes
  }

}