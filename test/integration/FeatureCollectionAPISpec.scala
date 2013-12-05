package integration

import nosql.json.Gen
import nosql.json.Gen._
import play.api.libs.json._
import play.api.http.Status._
import org.specs2.matcher.Matcher
import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class FeatureCollectionAPISpec extends InCollectionSpecification {


  def is = s2""" $sequential

     The FeatureCollection /download should:
       return 404 when the collection does not exist                              $e1
       return all elements when the collection does exist                         $e2

     The FeatureCollection /list should:
       return the objectscontained within the specified bbox as json object       $e3
       respond to the start query-param                                           $e4
       respond to the limit query-param                                           $e5
       support pagination                                                         $e6

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
      getDownload(testDbName, testColName).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeatures(features)))
      )
    }
  }

  def e3 = withTestFeatures(10, 10) {
      (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome( matchFeaturesInJson(featuresIn01)))
      )
    }


  def e4 = withTestFeatures(100, 10){
      (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome( (js: JsValue) =>
          (js must matchTotalInJson(100)) and (js must matchCountInJson(90))))
      )
    }

  def e5 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 10)).applyMatcher(
      res => (res.status must equalTo(OK)) and (res.responseBody must beSome((js: JsValue) =>
        (js must matchTotalInJson(100)) and (js must matchCountInJson(10))))
    )
  }

  def e6 = withTestFeatures(100, 10) {
      (bbox: String, featuresIn01: JsArray) => {
        val buffer = scala.collection.mutable.ListBuffer[JsValue]()
        for (start <- 0 to 90 by 10) {
          buffer += getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> start, "limit" -> 10)).responseBody.get
        }
        collectFeatures(buffer.toSeq) must matchFeatures(featuresIn01)
      }
    }

  def withTestFeatures[T](sizeInsideBbox: Int, sizeOutsideBbox: Int)( block: (String, JsArray) => T) = {
    val (featuresIn01, allFeatures) = featureArray("01", 100)
          .flatMap(f1 => featureArray("1", 200).map(f2 => (f1, f2 ++ f1))).sample.get
        val env: Envelope = "01"
        val bbox = s"${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY}"
        withFeatures(testDbName, testColName, allFeatures) {
          block(bbox, featuresIn01)
        }
  }

  def collectFeatures(listOfResponses: Seq[JsValue]) =
    listOfResponses.foldLeft( JsArray() ) ( (state, elem) =>  (elem \ "features").asOpt[JsArray] match {
        case Some(arr) => state ++ arr
        case _ => state
      }
    )

  def pruneSpecialProperties(js: JsValue) : JsValue = {
    val tr :Reads[JsObject] = (__ \ "_id").json.prune andThen ( __ \ "_mc").json.prune andThen ( __  \ "_bbox").json.prune
    js.transform(tr).asOpt.getOrElse(JsNull)
  }

  def matchFeaturesInJson(expected: JsArray) : Matcher[JsValue] = (
    (js: JsValue) => {
      val receivedFeatureArray = (js \ "features").as[JsValue]
      (receivedFeatureArray must matchFeatures(expected)).isSuccess

    }, "Featurecollection Json doesn't contain expected features")

  def matchTotalInJson(expectedTotal: Int)  : Matcher[JsValue] = (
    (recJs: JsValue) => {
      (( recJs \ "total").asOpt[Int] must beSome(expectedTotal)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for total field")

  def matchCountInJson(expectedCount: Int): Matcher[JsValue] = (
      (recJs: JsValue) => {
        (( recJs \ "count").asOpt[Int] must beSome( expectedCount)).isSuccess
      }, "FeatureCollection Json doesn't have expected value for count field")

  def matchFeatures(expected: JsArray) : Matcher[JsValue] = (
    (rec : JsValue) => rec match {
      case jsv: JsArray =>
        jsv.value.map(pruneSpecialProperties).toSet.equals(expected.value.toSet) //toSet so test in order independent

      case _ => false
    }, "Features don't match")







}
