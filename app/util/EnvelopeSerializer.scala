package util

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import play.api.libs.json._
import play.api.data.validation.ValidationError

/**
 * @author Karel Maesen, Geovise BVBA
 *
 */

//TODO apply/unapply methods should be simplified

object EnvelopeSerializer {

  private val pattern = "(\\d+):(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

  def apply(bbox: Envelope): String = {
    if (bbox == null) ""
    else {
      val srid = bbox.getCrsId.getCode
      val builder = new StringBuilder()
        .append(srid)
        .append(":")
        .append(List(bbox.getMinX, bbox.getMinY, bbox.getMaxX, bbox.getMaxY).mkString(","))
      builder.toString
    }
  }

  def unapply(str: String): Option[Envelope] =
    cleaned(str) match {
      case pattern(srid, minx, miny, maxx, maxy) => {
        try {
          Some(new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, CrsId.parse(srid)))
        } catch {
          case _: Throwable => None
        }
      }
      case _ => None
    }

  def cleaned(str: String): String = str.trim

  implicit val EnvelopeFormat = new Format[Envelope] {
    def reads(json: JsValue): JsResult[Envelope] = json match {
      case js : JsString => unapply(js.value).map( env => JsSuccess(env)).getOrElse(JsError("Invalid envelope string"))
      case _ => JsError(ValidationError("envelope must be a String"))
    }

    def writes(env: Envelope): JsValue = JsString(apply(env))
  }

}
