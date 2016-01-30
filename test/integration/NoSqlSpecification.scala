package integration

import org.specs2._
import org.specs2.main.{Arguments, ArgProperty}
import org.specs2.matcher.{Expectable, Matcher}

import org.specs2.specification.Step
import org.specs2.specification.core.{Env, SpecStructure, Fragments}
import play.api.{libs, Play}
import play.api.Play._
import play.api.libs.json.{Json, JsArray, JsObject, _}
import play.api.test.FakeApplication

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
abstract class NoSqlSpecification(app: FakeApplication = FakeApplication()) extends Specification {
  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  lazy val configuredDatabase  : String = app.configuration.getString("fs.db").getOrElse("mongodb")

  //override decorate so we can inject 'include' en 'sequential' arguments
  override def decorate(is: SpecStructure, env: Env) = {
    val dec = super.decorate( is, env )
    dec.setArguments(dec.arguments  <| args(include=configuredDatabase, sequential=true) )
  }

  override def map(fs: =>Fragments) =
      step(Play.start(app)) ^ tag(configuredDatabase) ^
        fs ^
      step(Play.stop(app)) ^ tag(configuredDatabase)

}

abstract class InDatabaseSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import integration.RestApiDriver._

  override def map(fs: => Fragments) =
      step( Play.start( app ) ) ^ tag( configuredDatabase ) ^
      step( makeDatabase( testDbName ) ) ^ tag( configuredDatabase ) ^
      fs ^
      step( dropDatabase( testDbName ) ) ^ tag( configuredDatabase ) ^
      step( Play.stop( app ) ) ^ tag(configuredDatabase)
}

abstract class InCollectionSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import integration.RestApiDriver._

  override def map(fs: => Fragments) =
      step( Play.start( app ) ) ^ tag( configuredDatabase ) ^
      step( makeDatabase( testDbName ) ) ^ tag( configuredDatabase ) ^
      step( makeCollection( testDbName, testColName ) ) ^ tag( configuredDatabase ) ^
      fs ^
      step( dropDatabase( testDbName ) ) ^ tag( configuredDatabase ) ^
      step( Play.stop( app ) ) ^ tag( configuredDatabase )



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
    }, s"Featurecollection Json doesn't contain expected features (${Json.stringify(expected)})")

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
      val optTotalReceived = (recJs \ "total").asOpt[Int]
      ( optTotalReceived must beSome(expectedTotal)).isSuccess
    }, s"FeatureCollection Json doesn't have expected value for total field ($expectedTotal).")

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


