package controllers

import org.geolatte.geom.curve.{MortonContext, MortonCode}
import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.nosql.mongodb._
import com.mongodb.casbah.MongoClient
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}
import play.api.Logger
import util.QueryParam
import play.api.libs.iteratee.Enumerator
import concurrent.Future
import play.api.http.MimeTypes

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
          case None => BadRequest(s"BadRequest ${queryStr}")
        }
    }

  def mkChunked(db: String, collection: String, window: Envelope) =  {

    val dataContent = toStream(
      Repository.query(db, collection, window)
    )

    Ok.stream(dataContent).as(MimeTypes.TEXT)
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

  //this needs to move to a service layer
  object Repository {

    //these are temporary -- need to be injected
    var wgs84 = CrsId.valueOf(4326)
    val mortoncode = new MortonCode(new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, wgs84), 8))
    val mongo = MongoClient()

    val jsonMapper = new JsonMapper()




    def query(database: String, collection: String, window: Envelope): Iterator[String] = {
      val coll = mongo(database)(collection)
      val src = MongoDbSource(coll, mortoncode)
      val now = System.currentTimeMillis
      for (f <- src.query(window))
        yield(jsonMapper.toJson(f) + " ")
//      val responseBuilder = new StringBuilder("{ 'total' : ")
//        .append(resultList.size)
//        .append(",")
//        .append("'items': [")
//        .append(resultList.map(jsonMapper.toJson(_)).mkString(","))
//        .append("]")
//        .append(", 'millis': ")
//        .append(System.currentTimeMillis - now)
//        .append("}")
//      responseBuilder.toString
    }

  }


  def toStream(it: Iterator[String]) = {

    def advance = if (it.hasNext) it.next.iterator else Iterator.empty

    var nextStrIt = advance

    def hasNext : Boolean = {
      nextStrIt.hasNext || {nextStrIt = advance; nextStrIt.hasNext}
    }

    val itStream = new java.io.InputStream {
      def read(): Int =
        if (hasNext) nextStrIt.next else -1
    }

    Enumerator.fromStream(itStream)

  }



}