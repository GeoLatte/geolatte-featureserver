package controllers




import org.geolatte.geom.curve.{MortonContext, MortonCode}
import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.nosql.mongodb._
import com.mongodb.casbah.MongoClient
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}

object FeatureCollection extends Controller {


  def query(db: String, collection: String) = Action { request =>
    Ok("Query string is: " + request.queryString.mkString)
  }

}

trait Temp {
  type EnvelopeCoords = (Double, Double, Double, Double)

  val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

  val bboxStr2Tuple = (s: String) => {
    s match {
      case bbox_pattern(minx, miny, maxx, maxy) => {
        try
          Some((minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble))
        catch {
          case _ : Throwable => None
        }
      }
      case _ => None
    }
  }


  //these are temporary -- need to be injected
  var wgs84 = CrsId.valueOf(4326)
  val mortoncode = new MortonCode(new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, wgs84), 8))
  val mongo = MongoClient()

  val jsonMapper = new JsonMapper()

  def toWindow(option: Option[(Double, Double, Double, Double)]) : Envelope = option match {
    case Some((minx, miny, maxx, maxy)) => new Envelope(minx, miny, maxx, maxy, wgs84)
    case None => throw new RuntimeException()
  }

  def createResponse(database: String, collection: String, bbox: Option[(Double, Double, Double, Double)]) = {
    try {
      val window = toWindow(bbox)
      val coll = mongo(database)(collection)
      val src = MongoDbSource(coll, mortoncode)
      val now = System.currentTimeMillis
      val resultList = src.query(window).toList
      val responseBuilder = new StringBuilder("{ 'total' : ")
        .append(resultList.size)
        .append(",")
        .append("'items': [")
        .append(resultList.map( jsonMapper.toJson(_) ).mkString(","))
        .append("]")
        .append(", 'millis': ")
        .append(System.currentTimeMillis - now)
        .append("}")


    } finally {
      //nothing yet
    }
  }


}