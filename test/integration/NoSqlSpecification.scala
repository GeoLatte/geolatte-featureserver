package integration

import org.specs2._
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.specification.TagFragments.Tag
import org.specs2.specification.{Fragments, Step}
import play.api.Play
import play.api.Play._
import play.api.libs.json.{JsArray, JsObject, _}
import play.api.test.FakeApplication

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
abstract class NoSqlSpecification(app: FakeApplication = FakeApplication()) extends Specification {
  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  lazy val configuredDatabase  : String = app.configuration.getString("fs.db").getOrElse("mongodb")

  override def map(fs: =>Fragments) =
    sequential ^
      args(include = configuredDatabase) ^
      Tag(configuredDatabase) ^ Step(Play.start(app)) ^
        fs ^
      Tag(configuredDatabase) ^ Step(Play.stop())

}

abstract class InDatabaseSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import integration.RestApiDriver._
  override def map(fs: =>Fragments) =
    sequential ^
    args(include = configuredDatabase) ^
      Tag(configuredDatabase) ^ Step(Play.start(app)) ^
      Tag(configuredDatabase) ^ Step(makeDatabase(testDbName)) ^
      fs ^
      Tag(configuredDatabase) ^ Step(dropDatabase(testDbName)) ^
      Tag(configuredDatabase) ^ Step(Play.stop())
}

abstract class InCollectionSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import integration.RestApiDriver._
  override def map(fs: =>Fragments) =
    sequential ^
    args(include = configuredDatabase) ^
      Tag(configuredDatabase) ^ Step(Play.start(app)) ^
      Tag(configuredDatabase) ^ Step(makeDatabase(testDbName)) ^
      Tag(configuredDatabase) ^ Step(makeCollection(testDbName, testColName)) ^
      fs ^
      Tag(configuredDatabase) ^ Step(dropDatabase(testDbName)) ^
      Tag(configuredDatabase) ^ Step(Play.stop())

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
      val lines = received.flatMap(l => received(0).split("\n")).map( _.trim )
      val header = lines(0)
      val numSeps = header.split(",").length - 1
      def countSeps(l:String) = l.filter( _ == ',').length
      header == expectedColumnHeader && lines.forall( countSeps(_)  == numSeps)
    }, "Featurecollection CSV doesn't contain expected columns, or has irregular structure")

  def matchTotalInJson(expectedTotal: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "total").asOpt[Int] must beSome(expectedTotal)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for total field")

  def matchCountInJson(expectedCount: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      ((recJs \ "count").asOpt[Int] must beSome(expectedCount)).isSuccess
    }, "FeatureCollection Json doesn't have expected value for count field")


  def verify(rec: JsValue, expected: JsArray, sortMatters : Boolean = false) = rec match {
    case jsv: JsArray =>
      val received = jsv.value.map(pruneSpecialProperties)
      val ok =
          if (sortMatters) received.equals(expected.value)
          else received.toSet.equals(expected.value.toSet) //toSet so test in order independent
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

  case class beSomeFeatures(expected: JsArray, sorted : Boolean = false) extends Matcher[Option[JsValue]] {
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


