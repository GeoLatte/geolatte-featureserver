package integration

import org.specs2._
import org.specs2.mutable.Around
import org.specs2.specification.{Step, Fragments, Scope}
import org.specs2.execute.{Result, AsResult}
import play.api.test.{Helpers, WithApplication, FakeApplication}
import play.api.Play
import integration.RestApiDriver._
import play.api.test.FakeApplication
import play.api.libs.json._
import play.api.test.FakeApplication
import play.api.test.FakeApplication
import play.api.libs.json.JsArray
import play.api.test.FakeApplication
import play.api.libs.json.JsObject
import org.specs2.matcher.Matcher

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
abstract class NoSqlSpecification(app: FakeApplication = FakeApplication()) extends Specification {
  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
       fs ^
       Step(Play.stop())

}

abstract class InDatabaseSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import RestApiDriver._
  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
      Step(makeDatabase(testDbName)) ^
      fs ^
      Step(dropDatabase(testDbName)) ^
      Step(Play.stop())
}

abstract class InCollectionSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import RestApiDriver._
  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
    Step(makeDatabase(testDbName)) ^
    Step(makeCollection(testDbName, testColName)) ^
    fs ^
    Step(dropDatabase(testDbName)) ^
    Step(Play.stop())

  //utility and matcher definitions

  def pruneSpecialProperties(js: JsValue): JsValue = {
    val tr: Reads[JsObject] = (__ \ "_id").json.prune andThen (__ \ "_mc").json.prune andThen (__ \ "_bbox").json.prune
    js match {
      case js: JsObject => js.transform(tr).asOpt.getOrElse(JsNull)
      case js: JsArray => JsArray(js.value.map(el => pruneSpecialProperties(el)))
      case _ => sys.error("Can't prune value which isn't JSON object or array.")
    }
  }

  def matchFeaturesInJson(expected: JsArray): Matcher[JsValue] = (
    (js: JsValue) => {
      val receivedFeatureArray = (js \ "features").as[JsValue]
      (receivedFeatureArray must matchFeatures(expected)).isSuccess

    }, "Featurecollection Json doesn't contain expected features")

  def matchTotalInJson(expectedTotal: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "total").asOpt[Int] must beSome(expectedTotal)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for total field")

  def matchCountInJson(expectedCount: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "count").asOpt[Int] must beSome(expectedCount)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for count field")

  def matchFeatures(expected: JsArray): Matcher[JsValue] = (
    (rec: JsValue) => rec match {
      case jsv: JsArray =>
        jsv.value.map(pruneSpecialProperties).toSet.equals(expected.value.toSet) //toSet so test in order independent

      case _ => false
    }, "Features don't match")

}


