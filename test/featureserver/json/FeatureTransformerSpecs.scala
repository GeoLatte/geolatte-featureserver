package featureserver.json

import featureserver.json.Gen._
import featureserver.{ FeatureTransformers, Metadata }
import org.geolatte.geom.DimensionalFlag._
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.specs2.mutable.Specification
import play.api.libs.json._

import scala.language.implicitConversions
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
class FeatureTransformerSpecs extends Specification {

  val testSize = 5
  val numPointsPerLineString = 2
  val crs = CrsId.valueOf(3000)
  val maxExtent = new Envelope(0, 0, 1000, 1000, crs)
  val indexLevel = 4
  implicit val mortonCode = new MortonCode(new MortonContext(maxExtent, indexLevel))

  "An IdValidator" should {

    val pnt = point(d2D)("02").sample.get
    val prop = properties("foo" -> Gen.oneOf("bar", "bar2"), "num" -> Gen.oneOf(1, 2, 3))

    "validate jsons with numeric ID-properties iff metadata indicates decimal id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "json.as(validator) returns json if it validates" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.as(validator) must_== json
    }

    "validate jsons with string ID-properties iff metadata indicates text id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "text")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "not validate jsons with string ID-properties iff metadata indicates decimal-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

    "json.as(validator) throw exceptions if it doesn't validates" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      Try { json.as(validator) } must beFailedTry
    }

    "not validate jsons with numeric ID-properties iff metadata indicates textl id-type" in {
      val md = Metadata("col", new Envelope(0, 0, 1000, 1000), 8, "text")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

  }

}

