package featureserver

import org.geolatte.geom._
import org.geolatte.geom.curve.{ MortonCode, MortonContext }
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._
/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
object FeatureTransformers {

  import featureserver.json.GeometryReaders._

  /**
   * Extracts the Envelope from the GeoJson
   * @param extent the spatial extent
   * @return
   */
  def envelopeTransformer(extent: Envelope): Reads[Polygon] =
    (__ \ 'geometry \ 'coordinates).json.pick[JsArray] andThen
      PositionReads.map { pos =>
        pos.boundingBox
      }.map { bbox =>
        toPolygon(bbox.toEnvelope(extent.getCrsId))
      }

  def toPolygon(envelope: Envelope): Polygon = {
    val builder = PointSequenceBuilders.fixedSized(5, DimensionalFlag.d2D, envelope.getCrsId)
    builder.add(envelope.getMinX, envelope.getMinY)
    builder.add(envelope.getMaxX, envelope.getMinY)
    builder.add(envelope.getMaxX, envelope.getMaxY)
    builder.add(envelope.getMinX, envelope.getMaxY)
    builder.add(envelope.getMinX, envelope.getMinY)
    new Polygon(builder.toPointSequence)
  }

  def validator(idType: String): Reads[JsObject] = idType match {
    case "decimal" => (__ \ "id").read[Long] andKeep __.read[JsObject]
    case "text" => (__ \ "id").read[String] andKeep __.read[JsObject]
    case _ => throw new IllegalArgumentException("Invalid metadata")
  }

}
