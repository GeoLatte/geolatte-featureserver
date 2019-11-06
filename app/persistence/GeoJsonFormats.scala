package persistence

import org.geolatte.geom
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.json.{ Json, _ }

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
object GeoJsonFormats {

  implicit val EnvelopeFormats: Format[Envelope] = new Format[Envelope] {

    def reads(json: JsValue): JsResult[Envelope] =
      scala.util.Try {
        val extent = (json \ "envelope").get
        val crs = (json \ "crs").as[Int]
        toEnvelope(extent, CrsId.valueOf(crs))
      }.recover {
        case t: Throwable => JsError(JsonValidationError(t.getMessage))
      }.get

    def writes(e: Envelope): JsValue = Json.obj(
      "crs" -> e.getCrsId.getCode,
      "envelope" -> (if (e.isEmpty) {
        JsArray()
      } else {
        Json.arr(e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)
      }))

    def toEnvelope(jsValue: JsValue, crs: CrsId): JsResult[Envelope] = jsValue match {
      case array: JsArray if array.value.isEmpty => JsSuccess(Envelope.EMPTY)
      case array: JsArray => Try {
        val xmin = array.head.as[Double]
        val ymin = array.value(1).as[Double]
        val xmax = array.value(2).as[Double]
        val ymax = array.value(3).as[Double]
        JsSuccess(new Envelope(xmin, ymin, xmax, ymax, crs))
      }.getOrElse(JsError(JsonValidationError(s"Array $array can't be turned into a valid boundingbox")))
      case _ => JsError("Json value is not an array")
    }

  }

  implicit val crsWrites: Writes[CrsId] = (
    (__ \ "type").write[String] and
    (__ \ "properties" \ "name").write[String])((to: CrsId) => ("name", to.toString()))

  implicit val crsReads: Reads[CrsId] = (
    (__ \ "type").read[String] and
    (__ \ "properties" \ "name").read[String])((_: String, name: String) => CrsId.parse(name))

  //READS
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
      .map(geometries => new GeometryCollection(geometries.map(geometry => mkGeometryReads(crsId).reads(geometry).get)))

  def mkGeometryReads(defaultCrsId: CrsId): Reads[Geometry] = (
    (__ \ "type").read[String] and
    (__ \ "crs").readNullable[CrsId] and
    __.json.pick)((typeDiscriminator: String, crsOpt: Option[CrsId], js: JsValue) => {

      implicit val crsInUse: CrsId = crsOpt.getOrElse(defaultCrsId)
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

  implicit val geometryReads: Reads[Geometry] = mkGeometryReads(CrsId.UNDEFINED)

  /**
   * Extracts the Geometry from the GeoJson
   *
   * @return
   */
  val geoJsonGeometryReads: Reads[Geometry] = {
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

  def featureValidator(idType: String): Reads[JsObject] = idType match {
    case "decimal" => (__ \ "id").read[Long] andKeep __.read[JsObject]
    case "text" => (__ \ "id").read[String] andKeep __.read[JsObject]
    case _ => throw new IllegalArgumentException("Invalid metadata")
  }

  // alle GeoJsonTo classes delen deze properties
  private val baseGeoJsonToWrites = {
    (__ \ "type").write[String] and
      (__ \ "crs").write[CrsId] and
      (__ \ "bbox").write[Array[Double]]
  }

  implicit val pointWrites: Writes[Point] = new Writes[Point] {
    override def writes(o: Point): JsValue = {
      if (o.isEmpty) {
        JsNull
      } else {
        (
          baseGeoJsonToWrites and
          (__ \ "coordinates").write[Array[Double]])((p: Point) => {
            val coords = coordinates(p)
            val crs = p.getCrsId
            ("Point", crs, bbox(p), coords)
          }).writes(o)
      }
    }
  }

  implicit val linestringWrites: Writes[LineString] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Double]]])((l: LineString) => {
      val crs = l.getCrsId
      val coords = coordinates(l)
      ("LineString", crs, bbox(l), coords)
    })

  implicit val polygonWrites: Writes[Polygon] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Double]]]])((l: Polygon) => {
      val crs = l.getCrsId
      val coords = coordinates(l)
      ("Polygon", crs, bbox(l), coords)
    })

  implicit val multilinestringWrites: Writes[MultiLineString] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Double]]]])((m: MultiLineString) => {
      val crs = m.getCrsId
      val coords = coordinates(m)
      ("MultiLineString", crs, bbox(m), coords)
    })

  implicit val multipolygonWrites: Writes[MultiPolygon] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Array[Double]]]]])((m: MultiPolygon) => {
      val crs = m.getCrsId
      val coords = coordinates(m)
      ("MultiPolygon", crs, bbox(m), coords)
    })

  private def bbox(input: Geometry): Array[Double] = {
    if (input == null || input.isEmpty) {
      Array()
    } else {
      val env = input.getEnvelope
      Array(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
  }

  private def coordinates(p: Point): Array[Double] = {
    p.getDimensionalFlag match {
      case DimensionalFlag.d2D => Array(p.getX, p.getY)
      case DimensionalFlag.d3D => Array(p.getX, p.getY, p.getZ)
      case DimensionalFlag.d3DM => Array(p.getX, p.getY, p.getZ, p.getM)
      case DimensionalFlag.d2DM => Array(p.getX, p.getY, 0, p.getM)
    }
  }

  private def coordinates(l: LineString): Array[Array[Double]] = {
    import scala.collection.JavaConverters._
    val points = l.getPoints.iterator().asScala.map(p =>
      coordinates(p))
    points.toArray
  }

  private def coordinates(l: Polygon): Array[Array[Array[Double]]] = {
    import scala.collection.JavaConverters._
    val points = l.iterator().asScala.map(p =>
      coordinates(p))
    points.toArray
  }

  private def coordinates(m: MultiLineString): Array[Array[Array[Double]]] = {
    (0 until m.getNumGeometries).map(i => {
      coordinates(m.getGeometryN(i))
    }).toArray
  }

  private def coordinates(m: MultiPolygon): Array[Array[Array[Array[Double]]]] = {
    (0 until m.getNumGeometries).map(i => {
      coordinates(m.getGeometryN(i))
    }).toArray
  }

  implicit val geometryWrites: Writes[Geometry] = new Writes[Geometry] {
    def writes(geom: Geometry) = {
      geom match {
        case x: Point => pointWrites.writes(x)
        case x: LineString => linestringWrites.writes(x)
        case x: Polygon => polygonWrites.writes(x)
        case x: MultiLineString => multilinestringWrites.writes(x)
        case x: MultiPolygon => multipolygonWrites.writes(x)
        case x: GeometryCollection => geometryCollectionWrites.writes(x)
      }
    }
  }

  implicit lazy val geometryCollectionWrites: Writes[GeometryCollection] = (
    baseGeoJsonToWrites and
    (__ \ "geometries").write[Array[JsValue]])((g: GeometryCollection) => {
      val crs = g.getCrsId
      val geometries = g.iterator().toList
      ("GeometryCollection", crs, bbox(g), geometries.map(geometryWrites.writes).toArray[JsValue])
    })

}
