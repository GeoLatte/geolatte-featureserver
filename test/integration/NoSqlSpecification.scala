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
import org.specs2.matcher.{Expectable, Matcher}

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
      case _ => sys.error(s"Can't prune value which isn't JSON object or array: ${Json.stringify(js)}")
    }
  }

  def matchFeaturesInJson(expected: JsArray): Matcher[JsValue] = (
    (js: JsValue) => {
      val receivedFeatureArray = (js \ "features").as[JsValue]
      (receivedFeatureArray must beFeatures(expected)).isSuccess

    }, "Featurecollection Json doesn't contain expected features")

  def matchFeaturesInCsv(expectedColumnHeader: String): Matcher[Seq[String]] = (
    (received: Seq[String]) => {
      println("REC:" + received(0))
      received(0).split("\n")(0) == expectedColumnHeader
    }, "Featurecollection CSV doesn't contain expected columns")

  def matchTotalInJson(expectedTotal: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "total").asOpt[Int] must beSome(expectedTotal)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for total field")

  def matchCountInJson(expectedCount: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "count").asOpt[Int] must beSome(expectedCount)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for count field")


  def verify(rec: JsValue, expected: JsArray) = rec match {
    case jsv: JsArray =>
      val received = jsv.value.map(pruneSpecialProperties)
      val ok = received.toSet.equals(expected.value.toSet) //toSet so test in order independent
      val msg = if (!ok) {
        (for (f <- received if !expected.value.contains(f)) yield f).headOption.
          map(f => s" e.g. ${Json.stringify(f)}\nnot found among expected features. Example of expected:\n" +
          s"${expected.value.headOption.getOrElse("<None expected.>")}")
          .getOrElse("<Missing object in received>")
      } else "Received array matches expected"
      (ok, msg)
    case _ => (false, "Did not receive array")
  }

  case class beFeatures(expected: JsArray) extends Matcher[JsValue] {
    def apply[J <: JsValue](r: Expectable[J]) = {
      lazy val (succ, msg) = verify(r.value, expected)
      result(succ,
        msg,
        msg,
        r)
    }
  }

  case class beSomeFeatures(expected: JsArray) extends Matcher[Option[JsValue]] {
    def apply[J <: Option[JsValue]](r: Expectable[J]) = {
      lazy val (succ, msg) = if (!r.value.isDefined)
        (false, "Expected Some(<features>), received None")
        else verify(r.value.get, expected)
      result(succ,
        "received Some(<features>) with features matching expected",
        msg,
        r)
    }
  }


}


