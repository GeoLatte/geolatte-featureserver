package persistence.json

import org.geolatte.geom.DimensionalFlag._
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.specs2.mutable.Specification
import persistence.json.Gen._
import persistence.{ GeoJsonFormats, Metadata }
import play.api.libs.json._
import scala.collection.JavaConversions._

import scala.language.implicitConversions
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
class GeoJsonFormatSpecs extends Specification {

  val testSize = 5
  val numPointsPerLineString = 2
  val crs = CrsId.valueOf(3000)
  val maxExtent = new Envelope(0, 0, 1000, 1000, crs)
  val indexLevel = 4
  implicit val mortonCode = new MortonCode(new MortonContext(maxExtent, indexLevel))

  "An Feature Validator" should {

    val pnt = point(d2D)("02").sample.get
    val prop = properties("foo" -> Gen.oneOf("bar", "bar2"), "num" -> Gen.oneOf(1, 2, 3))

    "validate jsons with numeric ID-properties iff metadata indicates decimal id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "json.as(featureValidator) returns json if it validates" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.as(validator) must_== json
    }

    "validate jsons with string ID-properties iff metadata indicates text id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "text")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "not validate jsons with string ID-properties iff metadata indicates decimal-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

    "json.as(featureValidator) throw exceptions if it doesn't validates" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      Try {
        json.as(validator)
      } must beFailedTry
    }

    "not validate jsons with numeric ID-properties iff metadata indicates textl id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "text")
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

  }

  "the GeoJsonReader" should {

    import GeoJsonFormats._
    val jsonGC = """{"type":"GeometryCollection","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"geometries":[{"type":"Point","bbox":[173369.86,175371.1,173369.86,175371.1],"coordinates":[173369.86,175371.1]}]}"""

    "read 2D Points " in {
      val pnt = point(d2D)("00").sample get
      val json = Json.toJson(pnt)
      val rec = json.as[Geometry]
      matchCrs(rec, json) and matchType(json, "Point") and matchCoordinate(rec.asInstanceOf[Point], json)
    }

    "read 2D GeometryCollections" in {
      val gc = geometryCollection(2, d2D)("00").sample.get
      val json = Json.toJson(gc)
      val rec = json.as[Geometry]
      matchCrs(rec, json)

    }

    "parse correctly a GeometryCollection met enkel CRS op hoogste niveau" in {
      val json = Json.parse(jsonGC)
      val gc = json.as[Geometry]
      gc.getSRID must_=== 31370
    }

  }

  "the GeoJsonWrites" should {

    import GeoJsonFormats._

    "write 2D points " in {
      val pnt = point(d2D)("00").sample.get
      val json = Json.toJson(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)

    }

    "write 3DM points" in {
      val pnt = point(d3DM)("00").sample.get
      val json = Json.toJson(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)
    }

    "write 2DM points" in {
      val pnt = point(d2DM)("00").sample.get
      val json = Json.toJson(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)
    }

    "write 2D lineStrings" in {
      val ln = lineString(4, d2D)("00").sample.get
      val json = Json.toJson(ln)
      matchCrs(ln, json) and matchType(json, "LineString") and matchBbox(ln, json) and matchCoordinates(ln, json)
    }

    "write 2D polygons" in {
      val p = polygon(12)("00").sample.get
      val json = Json.toJson(p)
      matchCrs(p, json) and matchType(json, "Polygon") and matchBbox(p, json) and matchCoordinates(p, json)
    }

    "write geometryCollections" in {
      val gc = geometryCollection(2, d3DM)("00").sample.get
      val json = Json.toJson(gc)
      matchCrs(gc, json) and matchBbox(gc, json) and matchType(json, "GeometryCollection") and (
        (json \ "geometries").as[JsArray].value.size must_== 2
      )
    }

  }

  "the GeoJsonReaders" should {

    import GeoJsonFormats._

    "read 2D points" in {
      val pnt = point(d2D)("00").sample.get
      val json = Json.toJson(pnt)
      json.as[Geometry] must_=== pnt
    }

    "read 3DM points" in {
      val pnt = point(d3DM)("00").sample.get
      val json = Json.toJson(pnt)
      json.as[Geometry] must_=== pnt
    }

    "read GeometryCollections" in {
      val gc = geometryCollection(2, d3D)("00").sample.get
      val json = Json.toJson(gc)
      json.as[Geometry] must_=== gc
    }

  }

  private def matchCoordinate(pnt: Point, json: JsValue) = {
    val jsArr: JsArray = coordianteToJsArray(pnt)
    (json \ "coordinates").get must_=== jsArr
  }

  private def coordianteToJsArray(pnt: Point) =
    pnt.getDimensionalFlag match {
      case DimensionalFlag.d2D => Json.arr(pnt.getX, pnt.getY)
      case DimensionalFlag.d3D => Json.arr(pnt.getX, pnt.getY, pnt.getZ)
      case DimensionalFlag.d3DM => Json.arr(pnt.getX, pnt.getY, pnt.getZ, pnt.getM)
      case DimensionalFlag.d2DM => Json.arr(pnt.getX, pnt.getY, 0, pnt.getM)
    }

  private def matchCoordinates(line: LineString, json: JsValue) = {
    (json \ "coordinates").get must_=== pointsToJsArray(line.getPoints)
  }

  private def pointsToJsArray(ps: PointSequence): JsArray = {
    JsArray(ps.toList.map(coordianteToJsArray))
  }

  private def matchCoordinates(p: Polygon, json: JsValue) = {
    val interior = 0.until(p.getNumInteriorRing).map(i => pointsToJsArray(p.getInteriorRingN(i).getPoints))
    (json \ "coordinates").get must_=== JsArray(
      Seq(pointsToJsArray(p.getExteriorRing.getPoints)) ++ interior
    )
  }

  private def matchBbox(pnt: Geometry, json: JsValue) = {
    (json \ "bbox").get must_=== {
      val e = pnt.getEnvelope
      Json.arr(e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)
    }
  }

  private def matchType(json: JsValue, typeStr: String) = {
    (json \ "type").get must_=== JsString(typeStr)
  }

  private def matchCrs(geom: Geometry, json: JsValue) = {
    (json \ "crs" \ "properties" \ "name").get must_== JsString(geom.getCrsId.toString)
  }
}

