package util

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId

/**
 * @author Karel Maesen, Geovise BVBA
 *
 */

//TODO -- must be moved to utility module - shared by Mongodb, application and other modules.

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

}
