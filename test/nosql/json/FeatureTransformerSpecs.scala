package nosql.json

import nosql.{Metadata, FeatureTransformers}
import nosql.json.Gen._
import nosql.json.GeometryReaders._
import nosql.mongodb.SpecialMongoProperties
import org.geolatte.geom.DimensionalFlag._
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.specs2.mutable.Specification
import play.api.libs.json.{JsObject, JsString, _}

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
  val maxExtent = new Envelope(0,0,1000,1000, crs)
  val indexLevel = 4
  implicit val mortonCode = new MortonCode( new MortonContext(maxExtent, indexLevel) )


  "A FeatureIndexingTranformer" should {

    val transfo = FeatureTransformers.mkFeatureIndexingTranformer(maxExtent, 4)

    "add for 2D point features the correct _mc and _bbox properties" in {

      val pnt = point(d2D)("02").sample.get
      val prop = properties( "foo" -> Gen.oneOf("bar", "bar2") , "num" -> Gen.oneOf(1,2,3))
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      val transformResult = json.transform( transfo )
      val expectedExtent = Json.toJson[Extent](pnt.getEnvelope)
      val expectedMc = JsString(mortonCode.ofGeometry(pnt))

      def extract(r : JsResult[JsObject], p : String) = r match {
        case JsSuccess(o, _) => (o \ p).asOpt[JsValue]
        case _ => None
      }

      (transformResult must beAnInstanceOf[JsSuccess[JsObject]]) and
          ( extract(transformResult, "properties") must beSome) and
          ( extract(transformResult, SpecialMongoProperties.MC) must beSome(expectedMc)) and
          ( extract(transformResult, SpecialMongoProperties.BBOX) must beSome(expectedExtent))

    }

    "add for 3DM point features the correct _mc and _bbox properties" in {

      val pnt = point(d3DM)("02").sample.get
      val prop = properties( "foo" -> Gen.oneOf("bar", "bar2") , "num" -> Gen.oneOf(1,2,3))
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      val transformResult = json.transform( transfo )
      val expectedExtent = Json.toJson[Extent](pnt.getEnvelope)
      val expectedMc = JsString(mortonCode.ofGeometry(pnt))

      def extract(r : JsResult[JsObject], p : String) = r match {
        case JsSuccess(o, _) => (o \ p).asOpt[JsValue]
        case _ => None
      }

      (transformResult must beAnInstanceOf[JsSuccess[JsObject]]) and
        ( extract(transformResult, "properties") must beSome) and
        ( extract(transformResult, SpecialMongoProperties.MC) must beSome(expectedMc)) and
        ( extract(transformResult, SpecialMongoProperties.BBOX) must beSome(expectedExtent))

    }

  }

  "An IdValidator" should {


    val pnt = point(d2D)("02").sample.get
    val prop = properties( "foo" -> Gen.oneOf("bar", "bar2") , "num" -> Gen.oneOf(1,2,3))


    "validate jsons with numeric ID-properties iff metadata indicates decimal id-type" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "json.as(validator) returns json if it validates" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.as(validator) must_== json
    }

    "validate jsons with string ID-properties iff metadata indicates text id-type" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "text")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator) must_== JsSuccess(json)
    }

    "not validate jsons with string ID-properties iff metadata indicates decimal-type" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

    "json.as(validator) throw exceptions if it doesn't validates" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "decimal")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.idString, Gen(pnt), prop)
      val json = pf.sample.get
      Try{json.as(validator)} must beFailedTry
    }

    "not validate jsons with numeric ID-properties iff metadata indicates textl id-type" in {
      val md = Metadata("col", new Envelope(0,0,1000,1000), 8, "text")
      val validator = FeatureTransformers.validator(md.idType)
      val pf = geoJsonFeature(Gen.id, Gen(pnt), prop)
      val json = pf.sample.get
      json.validate(validator).isInstanceOf[JsError]
    }

  }

}


