package nosql.json

import org.specs2.mutable.Specification
import play.api.libs.json._
import nosql.json.GeometryReaders._
import org.geolatte.geom.builder.DSL._
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/29/13
 */
class GeometryReadersSpec extends Specification {

  "The CRS Reader " should {

    "read a valid named CRS representation" in {
      val crs: CrsId = crsJson.validate[CrsId].asOpt.get
      crs.getCode == 31370
    }

    "return a JsError when validating an invalid CRS representation" in {
      val result = crsInvalidJson.validate[CrsId]
      result match {
        case _: JsError => success
        case _ => failure
      }
    }

    "return a JsError with a ValidationError when validating an CRS with an unparseable EPSG string" in {
      val result = crsUnparseableEPGSJson.validate[CrsId]
      result match {
        case err: JsError => success
        case _ => failure
      }
    }

  }

  "The PointsReader" should {

    "read a valid point Position" in {
        val json = Json.parse("[84.5,23.3]")
        val result = json.validate[Positions].asOpt.get
        result must_== new Position(84.5, 23.3)
    }

    "read a valid array of point positions" in {
      val json = Json.parse("[[80.0, 20.3], [90.0, 30.0]]")
      val result = json.validate[Positions].asOpt.get
      result must_== PositionList(List(new Position(80.0, 20.3), new Position(90.0, 30.0)))
    }

    "read a valid array of array of point positions" in {
      val json = Json.parse("[[[80.0, 20.3], [90.0, 30.0]], [[80.0, 20.3], [90.0, 30.0]]]")
      val result = json.validate[Positions].asOpt.get
      val pl = PositionList(List(new Position(80.0, 20.3), new Position(90.0, 30.0)))
      result must_== PositionList(List(pl, pl))
    }

  }

  "The GeoJSon reader" should {

    implicit val defaultGeometryReaders = GeometryReads( CrsId.valueOf(4326) )

    "read a valid POINT GeoJson" in {
      val geom = geomJsonPnt.validate[Geometry].asOpt.get
      geom.getGeometryType == GeometryType.POINT && geom.getPointN(0).getX == -87.067872
    }

    "read a valid LineString GeoJson" in {
      val result = geomJsonLineString.validate[Geometry].asOpt.get
      result.equals(linestring(4326, c(-87.067872, 33.093221), c(90.2, 40.0)))
    }

    "read a valid MultiLineString GeoJson " in {
      val result = geomJsonMultiLineString.validate[Geometry].asOpt.get
      result.equals(multilinestring(4326, linestring(c(-87.067872, 33.093221), c(90.2, 40.0)), linestring(c(-87.067872, 33.093221), c(90.2, 40.0))))
    }

    "use the default crs defined in the readers as an implicit parameter when the Geometry has no CRS field" in {
      val geom = geomJsonPnt.validate[Geometry].asOpt.get
      geom.getCrsId must_== CrsId.valueOf(4326)
    }

    "use the crs field when present" in {
      val geom = geomJsonPntWithCrs.validate[Geometry].asOpt.get
      geom.getCrsId must_== CrsId.valueOf(31370)
    }

    "return a JsError when the coordinates array is invalid" in {
         val json = Json.parse("""{"type" : "MultiLineString", "coordinates" : [[[80.0, 20.3], 99999], [[80.0, 20.3], [90.0, 30.0]]]}""")
         val result = json.validate[Geometry]
         result match {
           case _ : JsError => success
           case _ => failure
         }
       }
  }

  "The GeoJSon reader" should {

    "Use the CrsId  declared when creating the GeometryReads" in {
      implicit val geometryReaders = GeometryReads(CrsId.valueOf(31370))
      val result = geomJsonLineString.validate[Geometry].asOpt.get
      result.getCrsId.equals(CrsId.valueOf(31370))
    }
  }

  // TEST DATA

  val crsJson = Json.parse( """{"type":"name","properties":{"name":"EPSG:31370"}}""")

  val crsInvalidJson = Json.parse( """{"type":"name","something-else":{"name":"EPSG:4326"}}""")

  val crsUnparseableEPGSJson = Json.parse( """{"type":"name","properties":{"name":"no valid code"}}""")

  val geomJsonPnt = Json.obj("type" -> "Point", "coordinates" -> Json.arr(-87.067872, 33.093221))

  val geomJsonPntWithCrs = Json.obj("type" -> "Point", "crs" -> crsJson, "coordinates" -> Json.arr(-87.067872, 33.093221))

  val geomJsonLineString = Json.obj("type" -> "LineString",
    "coordinates" -> Json.arr(Json.arr(-87.067872, 33.093221), Json.arr(90.2, 40.0)))

  val geomJsonMultiLineString = Json.obj("type" -> "MultiLineString",
    "coordinates" -> Json.arr(
      Json.arr(
        Json.arr(-87.067872, 33.093221), Json.arr(90.2, 40.0))
      ,
      Json.arr(
        Json.arr(-87.067872, 33.093221), Json.arr(90.2, 40.0))
    ))

  val jsFeature = Json.obj("id" -> 1,
    "properties" -> Json.obj("foo" -> "bar", "bar" -> Json.obj("inl2" -> 3)),
    "geometry" -> geomJsonLineString)

  val jsNoIdFeature = Json.obj("properties" -> Json.obj("foo" -> "bar", "bar" -> Json.obj("inl2" -> 3)),
    "geometry" -> geomJsonLineString)

  val noGeometry = Json.obj("properties" -> Json.obj("foo" -> "bar", "bar" -> Json.obj("inl2" -> 3))
  )

  val noProperties = Json.obj("geometry" -> geomJsonLineString)

}
