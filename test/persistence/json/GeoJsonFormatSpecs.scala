package persistence.json

import org.geolatte.geom._
import org.geolatte.geom.crs.{ CrsRegistry, ProjectedCoordinateReferenceSystem }
import org.geolatte.geom.curve._
import org.specs2.mutable.Specification
import persistence.json.Gen._
import persistence.{ GeoJsonFormats, Metadata }
import play.api.libs.json._

import scala.language.implicitConversions
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
class GeoJsonFormatSpecs extends Specification {

  val testSize = 5
  val numPointsPerLineString = 2
  // SRID 31370 is Belgian Lambert 72 — a projected (C2D) CRS that ships with
  // geolatte-geom 1.11. Replaces the old test SRID 3000 which was a fake.
  // Use the typed projected CRS lookup since MortonCode/MortonContext require
  // P <: C2D and the implicit conversion in Gen.scala goes through MortonCode.
  val crs: ProjectedCoordinateReferenceSystem =
    CrsRegistry.getProjectedCoordinateReferenceSystemForEPSG(31370)
  implicit val maxExtent: Envelope[C2D] = new Envelope[C2D](0, 0, 1000, 1000, crs)
  val indexLevel = 4
  implicit val mortonCode: MortonCode[C2D] =
    new MortonCode[C2D](new MortonContext[C2D](maxExtent, indexLevel))

  "An Feature Validator" should {

    val pnt = point(d2D)("02").sample.get
    val prop = properties("foo" -> Gen.oneOf("bar", "bar2"), "num" -> Gen.oneOf(1, 2, 3))

    "validate jsons with numeric ID-properties iff metadata indicates decimal id-type" in {
      val md = Metadata("col", maxExtent, 8, "decimal", encodedAsJsonb = false)
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "json.as(featureValidator) returns json if it validates" in {
      val md = Metadata("col", maxExtent, 8, "decimal", encodedAsJsonb = false)
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.as(validator) must_== json
    }

    "validate jsons with string ID-properties iff metadata indicates text id-type" in {
      val md = Metadata("col", maxExtent, 8, "text", encodedAsJsonb = false)
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "not validate jsons with string ID-properties iff metadata indicates decimal-type" in {
      val md = Metadata("col", maxExtent, 8, "decimal", encodedAsJsonb = false)
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

    "json.as(featureValidator) throw exceptions if it doesn't validates" in {
      val md = Metadata("col", maxExtent, 8, "decimal", encodedAsJsonb = false)
      val validator = GeoJsonFormats.featureValidator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      Try {
        json.as(validator)
      } must beFailedTry
    }

    "not validate jsons with numeric ID-properties iff metadata indicates textl id-type" in {
      val md = Metadata("col", maxExtent, 8, "text", encodedAsJsonb = false)
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
      val pnt = point(d2D)("00").sample.get
      val json = geometryWrites.writes(pnt)
      val rec = json.as[Geometry[_]]
      matchCrs(rec, json) and matchType(json, "Point") and matchCoordinate(rec.asInstanceOf[Point[_]], json)
    }

    "read 2D GeometryCollections" in {
      val gc = geometryCollection(2, d2D)("00").sample.get
      val json = geometryWrites.writes(gc)
      val rec = json.as[Geometry[_]]
      matchCrs(rec, json)
    }

    "parse correctly a GeometryCollection met enkel CRS op hoogste niveau" in {
      val json = Json.parse(jsonGC)
      val gc = json.as[Geometry[_]]
      gc.getSRID must_=== 31370
    }

  }

  "the GeoJsonWrites" should {

    import GeoJsonFormats._

    "write 2D points " in {
      val pnt = point(d2D)("00").sample.get
      val json = geometryWrites.writes(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)
    }

    "write 3DM points" in {
      val pnt = point(d3DM)("00").sample.get
      val json = geometryWrites.writes(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)
    }

    "write 2DM points" in {
      val pnt = point(d2DM)("00").sample.get
      val json = geometryWrites.writes(pnt)
      matchCrs(pnt, json) and matchType(json, "Point") and matchBbox(pnt, json) and matchCoordinate(pnt, json)
    }

    "write 2D lineStrings" in {
      val ln = lineString(4, d2D)("00").sample.get
      val json = geometryWrites.writes(ln)
      matchCrs(ln, json) and matchType(json, "LineString") and matchBbox(ln, json) and matchCoordinates(ln, json)
    }

    "write 2D polygons" in {
      val p = polygon(12)("00").sample.get
      val json = geometryWrites.writes(p)
      matchCrs(p, json) and matchType(json, "Polygon") and matchBbox(p, json) and matchCoordinates(p, json)
    }

    "write geometryCollections" in {
      val gc = geometryCollection(2, d3DM)("00").sample.get
      val json = geometryWrites.writes(gc)
      matchCrs(gc, json) and matchBbox(gc, json) and matchType(json, "GeometryCollection") and (
        (json \ "geometries").as[JsArray].value.size must_== 2
        )
    }

  }

  "the GeoJsonReaders" should {

    import GeoJsonFormats._

    "read 2D points" in {
      val pnt: Geometry[_] = point(d2D)("00").sample.get
      val json = geometryWrites.writes(pnt)
      json.as[Geometry[_]] must_== pnt
    }

    "read 3DM points" in {
      val pnt: Geometry[_] = point(d3DM)("00").sample.get
      val json = geometryWrites.writes(pnt)
      json.as[Geometry[_]] must_== pnt
    }

    "read GeometryCollections" in {
      val gc: Geometry[_] = geometryCollection(2, d3D)("00").sample.get
      val json = geometryWrites.writes(gc)
      json.as[Geometry[_]] must_== gc
    }

  }

  // -------------------------------------------------------------------------
  // Helpers — these compute the expected JSON shape using the same wire-format
  // rules as GeoJsonFormats (notably the 4-element [x,y,0,m] form for 2DM).
  // -------------------------------------------------------------------------

  private def positionToCoordArray(ps: PositionSequence[_], i: Int): Array[Double] = {
    val dim = ps.getCoordinateDimension
    val buf = new Array[Double](dim)
    ps.getCoordinates(i, buf)
    val cls = ps.getPositionClass.getSimpleName
    if (cls == "C2DM" || cls == "G2DM") {
      // 2DM: emit [x, y, 0, m] (4 elements with z=0)
      Array(buf(0), buf(1), 0.0, buf(2))
    } else {
      buf
    }
  }

  private def matchCoordinate(pnt: Point[_], json: JsValue) = {
    val arr = positionToCoordArray(pnt.getPositions, 0)
    val jsArr: JsArray = JsArray(arr.toIndexedSeq.map(d => JsNumber(d)))
    (json \ "coordinates").get must_=== jsArr
  }

  private def matchCoordinates(line: LineString[_], json: JsValue) = {
    (json \ "coordinates").get must_=== positionsToJsArray(line.getPositions)
  }

  private def positionsToJsArray(ps: PositionSequence[_]): JsArray = {
    val arrs = (0 until ps.size).map { i =>
      val coords = positionToCoordArray(ps, i)
      JsArray(coords.toIndexedSeq.map(d => JsNumber(d)))
    }
    JsArray(arrs)
  }

  private def matchCoordinates(p: Polygon[_], json: JsValue) = {
    val interior = (0 until p.getNumInteriorRing).map(i => positionsToJsArray(p.getInteriorRingN(i).getPositions))
    (json \ "coordinates").get must_=== JsArray(
      Seq(positionsToJsArray(p.getExteriorRing.getPositions)) ++ interior
    )
  }

  private def matchBbox(pnt: Geometry[_], json: JsValue) = {
    (json \ "bbox").get must_=== {
      val a = pnt.getEnvelope.toArray()
      Json.arr(a(0), a(1), a(2), a(3))
    }
  }

  private def matchType(json: JsValue, typeStr: String) = {
    (json \ "type").get must_=== JsString(typeStr)
  }

  private def matchCrs(geom: Geometry[_], json: JsValue) = {
    (json \ "crs" \ "properties" \ "name").get must_== JsString(geom.getCoordinateReferenceSystem.getCrsId.toString)
  }
}
