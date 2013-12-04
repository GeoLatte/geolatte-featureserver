package integration

import nosql.json.Gen
import nosql.json.Gen._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.http.Status._
import org.specs2.matcher.Matcher
import java.util.regex.MatchResult
import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class FeatureCollectionAPISpec extends InCollectionSpecification {


  def is = s2""" $sequential

     The FeatureCollection /download should:
       return 404 when the collection does not exist                $e1
       return all elements when the collection does exist           $e2

     The FeatureCollection /list should:
       return the objects contained within the specified bbox       $e3

  """

    //import default values
    import UtilityMethods._
    import RestApiDriver._

    //Generators for data
    val prop = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3))
    def geom(mc: String = "") = Gen.lineString(3)(mc)
    val idGen = Gen.id
    def feature(mc: String = "") = Gen.geoJsonFeature(idGen, geom(mc), prop)
    def featureArray(mc: String = "", size: Int = 10) = Gen.geoJsonFeatureArray(feature(mc), size)

  def e1 = getDownload(testDbName, "nonExistingCollection").applyMatcher( _.status must equalTo(NOT_FOUND))

  def e2 = {
    val features = featureArray(size = 10).sample.get
    withFeatures(testDbName, testColName, features){
      RestApiDriver.getDownload(testDbName, testColName).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeatures(features)))
      )
    }
  }

  def e3 = {
    val (featuresIn01, allFeatures) = featureArray("01", 10)
      .flatMap(f1 => featureArray("1", 20).map(f2 => (f1, f2 ++ f1))).sample.get
    val env: Envelope = "01"
    val bbox = s"${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY}"
    def test(js: JsValue) = (js \ "features") must matchFeatures(featuresIn01)
    withFeatures(testDbName, testColName, allFeatures) {
      getList(testDbName, testColName, Map("bbox" -> bbox)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome( (js: JsValue) => (js \ "features") must matchFeatures(featuresIn01)))
      )
    }
  }

  def pruneSpecialProperties(js: JsValue) : JsValue = {
    val tr :Reads[JsObject] = (__ \ "_id").json.prune andThen ( __ \ "_mc").json.prune andThen ( __  \ "_bbox").json.prune
    js.transform(tr).asOpt.getOrElse(JsNull)
  }

  def matchFeatures(expected: JsArray) : Matcher[JsValue] = (
    (rec : JsValue) => rec match {
      case jsv: JsArray =>
        jsv.value.map(pruneSpecialProperties).toSet.equals(expected.value.toSet) //toSet so test in order independent

      case _ => false
    }, "Features don't match")







}
