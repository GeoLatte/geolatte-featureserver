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

object FeatureCollection extends Controller {

  val definedQueryParams = List(
    //we leave at this stage bbox as a String parameter because the
    QueryParam.make("bbox", (s: String) => Some(s) )
  )

  //temporary fixed value
  val WGS_84 = CrsId.valueOf(4326)

  def query(db: String, collection: String) = Action { request =>
    val params = extract(request.queryString)
    Logger.info(s"Query string is ${request.queryString} on $db, collection $collection")

    Ok(s"Query string is ${params.get("bbox")} on $db, collection $collection")
  }

  def extract(queryParams: Map[String, Seq[String]]): Map[String, AnyRef] = {
    var result = Map[String, AnyRef]()
    for ( key <- definedQueryParams) {
      queryParams.get(key.name) match {
        case Some(seq) => result += ( key.name -> key.bind(seq.head))
        case _ =>     //do nothing
      }
    }
    result
  }


}


object Bbox {

  private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

  def apply (s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try
            Some(new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs))
          catch {
            case _ : Throwable => None
          }
        }
        case _ => None
      }
    }

}


trait Temp {
  type EnvelopeCoords = (Double, Double, Double, Double)




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