package persistence

import org.geolatte.geom
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import play.api.libs.functional.syntax._
import play.api.libs.json.{ Json, _ }

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
object FeatureTransformers {

  /**
   * Helpermethod that creates a geolatte pointsequence starting from an array containing coordinate arrays
   *
   * @param coordinates an array containing coordinate arrays
   * @return a geolatte pointsequence or null if the coordinatesequence was null
   */
  private def createPointSequence(coordinates: Array[Array[Double]])(implicit crsId: CrsId): PointSequence = {
    if (coordinates.isEmpty) {
      PointCollectionFactory.createEmpty
    } else {
      val df: DimensionalFlag = if (coordinates.head.length == 4) DimensionalFlag.d3DM
      else if (coordinates.head.length == 3) DimensionalFlag.d3D else DimensionalFlag.d2D
      val psb: PointSequenceBuilder = PointSequenceBuilders.variableSized(df, crsId)
      coordinates.foreach(coordinate => psb.add(coordinate))
      psb.toPointSequence
    }
  }

  implicit def pointReads(implicit crsId: CrsId): Reads[Point] =
    (__ \ "coordinates").read[Array[Double]]
      .map(createPoint(crsId, _))

  private def createPoint(crsId: CrsId, coordinates: Array[Double]) = {
    if (coordinates.length == 2) {
      Points.create2D(coordinates(0), coordinates(1), crsId)
    } else if (coordinates.length == 3) {
      Points.create3D(coordinates(0), coordinates(1), coordinates(2), crsId)
    } else {
      val z: Double = coordinates(2)
      val m: Double = coordinates(3)
      if (z.isNaN) {
        Points.create2DM(coordinates(0), coordinates(1), m, crsId)
      } else {
        Points.create3DM(coordinates(0), coordinates(1), z, m, crsId)
      }
    }
  }

  implicit def lineStringReads(implicit crsId: CrsId): Reads[LineString] =
    (__ \ "coordinates").read[Array[Array[Double]]]
      .map(coordinates => new LineString(createPointSequence(coordinates)))

  implicit def multiLineStringReads(implicit crsId: CrsId): Reads[MultiLineString] =
    (__ \ "coordinates").read[Array[Array[Array[Double]]]]
      .map(coordinates => new MultiLineString(coordinates.map(createPointSequence).map(new LineString(_))))

  private def createPolygon(coordinates: Array[Array[Array[Double]]])(implicit crsId: CrsId): geom.Polygon =
    new Polygon(coordinates.map(sequence => new LinearRing(createPointSequence(sequence))))

  implicit def polygonReads(implicit crsId: CrsId): Reads[Polygon] =
    (__ \ "coordinates").read[Array[Array[Array[Double]]]]
      .map(coordinates => createPolygon(coordinates))

  private def createMultiPoint(coordinates: Array[Array[Double]])(implicit crsId: CrsId): geom.MultiPoint =
    new MultiPoint(coordinates.map(co => createPoint(crsId, co)))

  implicit def multiPointReads(implicit crsId: CrsId): Reads[MultiPoint] =
    (__ \ "coordinates").read[Array[Array[Double]]]
      .map(createMultiPoint)

  implicit def multiPolygonReads(implicit crsId: CrsId): Reads[MultiPolygon] =
    (__ \ "coordinates").read[Array[Array[Array[Array[Double]]]]]
      .map(coordinates => new MultiPolygon(coordinates.map(sequence => createPolygon(sequence))))

  implicit def geometryCollectionReads(implicit crsId: CrsId): Reads[GeometryCollection] =
    (__ \ "geometries").read[Array[JsValue]]
      .map(geometries => new GeometryCollection(geometries.map(geometry => geometryReads.reads(geometry).get)))

  implicit def geometryReads(implicit crsId: CrsId): Reads[Geometry] = (
    (__ \ "type").read[String] and
    __.json.pick
  )((typeDiscriminator: String, js: JsValue) => {

      typeDiscriminator match {
        case "Point" => Json.fromJson[Point](js).get
        case "LineString" => Json.fromJson[LineString](js).get
        case "Polygon" => Json.fromJson[Polygon](js).get
        case "MultiPoint" => Json.fromJson[MultiPoint](js).get
        case "MultiLineString" => Json.fromJson[MultiLineString](js).get
        case "MultiPolygon" => Json.fromJson[MultiPolygon](js).get
        case "GeometryCollection" => Json.fromJson[GeometryCollection](js).get
      }
    })

  /**
   * Extracts the Envelope from the GeoJson
   *
   * @return
   */
  def geometryReads(extent: Envelope): Reads[Geometry] = {
    implicit val crsId: CrsId = extent.getCrsId
    (__ \ 'geometry).json.pick[JsObject] andThen geometryReads
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
