package controllers

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}
import play.api.Logger
import util.QueryParam
import play.api.libs.iteratee.Enumerator
import play.api.http.MimeTypes
import org.geolatte.common.Feature
import org.geolatte.scala.ChainedIterator
import repositories.MongoRepository

object FeatureCollection extends Controller {

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))
    val CRS = QueryParam("crs", (s: String) => Some(CrsId.parse(s)))
  }

    //temporary fixed value
    val WGS_84 = CrsId.valueOf(4326)

    def query(db: String, collection: String) = Action {
      request =>
        implicit val queryStr = request.queryString
        Logger.info(s"Query string ${queryStr} on $db, collection $collection")
        val windowOpt = Bbox(QueryParams.BBOX.extractOrElse(""), QueryParams.CRS.extractOrElse(WGS_84))
        windowOpt match {
          case Some(window) => mkChunked(db, collection, window)
          case None => BadRequest(s"BadRequest: No or invalid bbox parameter in query string.")
        }
    }

  def mkChunked(db: String, collection: String, window: Envelope) =  {
    try {
      val dataContent = toStream(MongoRepository.query(db, collection, window))
      Ok.stream(dataContent).as(MimeTypes.JSON)
    }
    catch {
      case ex : IllegalArgumentException => BadRequest(s"Error: " + ex.getMessage())
    }
  }

  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try
            Some(new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs))
          catch {
            case _: Throwable => None
          }
        }
        case _ => None
      }
    }

  }


  def toStream(features: Iterator[Feature]) = {

    import ChainedIterator._

    val START = List("{ \"items\": [").iterator
    val END = List("]}").iterator

    val jsonMapper = new JsonMapper()
    var count = 0
    val jsons = for (json <- features.map( jsonMapper.toJson(_)))
      yield (if (count == 0) {count += 1; json} else ","+json)

    val jsonStringIt = chain[String](START, jsons , END)

    def advance = if (jsonStringIt.hasNext) jsonStringIt.next.iterator else Iterator.empty

    var currentJson = advance

    def hasNext : Boolean = {
      currentJson.hasNext || {currentJson = advance; currentJson.hasNext}
    }

    val itStream = new java.io.InputStream {
      def read(): Int =
        if (hasNext) currentJson.next else -1
    }

    Enumerator.fromStream(itStream)

  }

}