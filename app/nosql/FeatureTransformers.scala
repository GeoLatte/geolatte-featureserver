package nosql

import nosql.json.GeometryReaders
import nosql.mongodb.SpecialMongoProperties._
import org.geolatte.geom._
import org.geolatte.geom.curve.{MortonCode, MortonContext}
import play.api.data.validation.ValidationError
import play.api.libs.json.Reads._
import play.api.libs.json._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
object FeatureTransformers {

  import nosql.json.GeometryReaders._

  def bboxAndMortonCode(implicit extent : Envelope, level: Int) = {

    //function from extents to their morton codes
    val mc = {
        val mc = new MortonCode(new MortonContext(extent, level))
        (bbox: Extent) => mc.ofEnvelope(bbox.toEnvelope(extent.getCrsId))
    }

    //get the coordinates array as a Positions, map to boundingbox and check if it falls
    //into specified extent, if so map to {_mc: .., _bbox: ...} JsObject
    ( __ \ 'geometry \ 'coordinates).json.pick[JsArray]  andThen
          PositionReads.map(pos => pos.boundingBox).filter(ValidationError("coordinates not within extent"))(
      bbox => extent.contains(bbox.toEnvelope(extent.getCrsId))
    ).map( bbox => Json.obj(MC -> mc(bbox), BBOX -> bbox))

  }

  /**
   * Creates a transformer that adds an _mc (Mortoncode)and _bbox (bounding box) to a GeoJson feature
   *
   * @param extent an Envelope that specifies maximum extent within which the geojson features must lie
   * @param level the index-level (used in the morton code calculation
   * @return a Reads[JsObject] for the transformation
   */
  def mkFeatureIndexingTranformer(implicit extent : Envelope, level: Int) : Reads[JsObject] =
    __.json.update( bboxAndMortonCode )


  /**
   * Extracts the Envelope from the GeoJson
   * @param extent
   * @return
   */
  def envelopeTransformer(extent: Envelope) : Reads[Polygon] =
    ( __ \ 'geometry \ 'coordinates ).json.pick[JsArray] andThen
      PositionReads.map {pos =>
        pos.boundingBox
      }.map { bbox =>
        toPolygon(bbox.toEnvelope(extent.getCrsId))
      }

  def toPolygon(envelope: Envelope) : Polygon = {
    val builder = PointSequenceBuilders.fixedSized(5, DimensionalFlag.d2D,envelope.getCrsId)
    builder.add(envelope.getMinX, envelope.getMinY)
    builder.add(envelope.getMaxX, envelope.getMinY)
    builder.add(envelope.getMaxX, envelope.getMaxY)
    builder.add(envelope.getMinX, envelope.getMaxY)
    builder.add(envelope.getMinX, envelope.getMinY)
    new Polygon(builder.toPointSequence)
  }

}
