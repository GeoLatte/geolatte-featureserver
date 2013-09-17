package org.geolatte.nosql.json

import org.specs2.mutable.Specification
import play.api.libs.json._
import org.geolatte.nosql.json.GeometryReaders._
import org.geolatte.geom.builder.DSL._
import org.geolatte.geom._
import org.geolatte.common.Feature
import org.specs2.execute.Result
import org.geolatte.geom.crs.CrsId

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/29/13
 */
class GeometryReadersSpec extends Specification {


  "The GeoJSon reader" should {

    implicit val defaultGeometryReaders = GeometryReads(CrsId.UNDEFINED)

    "read a valid POINT GeoJson" in {
      val geom = geomJsonPnt.validate[Geometry].asOpt.get
      geom.getGeometryType == GeometryType.POINT && geom.getPointN(0).getX == -87.067872
    }

    "read a valid LineString GeoJson" in {
      val result = geomJsonLineString.validate[Geometry].asOpt.get
      result.equals(linestring(-1, c(-87.067872, 33.093221), c(90.2, 40.0)))
    }

    "read a valid MultiLineString GeoJson " in {
      val result = geomJsonMultiLineString.validate[Geometry].asOpt.get
      result.equals(multilinestring(-1, linestring(c(-87.067872, 33.093221), c(90.2, 40.0)), linestring(c(-87.067872, 33.093221), c(90.2, 40.0))))
    }

  }

  "The GeoJSon reader" should {

    "Use the CrsId  declared when creating the GeometryReads" in {
          implicit val geometryReaders = GeometryReads(CrsId.valueOf(31370))
          val result = geomJsonLineString.validate[Geometry].asOpt.get
          println(result.getCrsId)
          result.getCrsId.equals(CrsId.valueOf(31370))
        }
  }

  "The Feature reader" should {

    implicit val defaultFeatureReads = FeatureReads(CrsId.UNDEFINED)

    def validateFeature(result: JsResult[Feature], withId: Boolean = true): Result = {
      val featureOpt = result.asOpt
      val classMatch = featureOpt must haveClass[Some[Feature]]
      val feature = featureOpt.get.asInstanceOf[Feature]
      val idMatch = if (withId) feature.getId must_== 1 else ok
      classMatch and idMatch and
        (feature.getGeometry must beEqualTo(linestring(-1, c(-87.067872, 33.093221), c(90.2, 40.0)))) and
        (feature.getProperty("foo") must_== "bar") and
        (feature.getProperty("bar").asInstanceOf[Map[String, AnyRef]] must havePair("inl2" -> 3))
    }

    "read a valid GeoJSON object having an id " in {
      val result = jsFeature.validate[Feature]
      validateFeature(result)
    }

    "read a valid GeoJSON object not having an id " in {
      val result = jsNoIdFeature.validate[Feature]
      validateFeature(result, withId = false)
    }

    "fail on reading an invalid GeoJson feature (no geometry)" in {
      val result = noGeometry.validate[Feature]
      result must beLike { case JsError(error) => ok }
    }

    "fail on reading an invalid GeoJson feature (no properties)" in {
          val result = noProperties.validate[Feature]
          result must beLike { case JsError(error) => ok }
        }


  }

  // TEST DATA

  val geomJsonPnt = Json.obj("type" -> "Point", "coordinates" -> Json.arr(-87.067872, 33.093221))

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
