package integration

import org.specs2._
import org.specs2.matcher.{ Expectable, Matcher }
import org.specs2.specification.core.{ Env, Fragments, SpecStructure }
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.{ JsArray, JsObject, Json, _ }

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
abstract class FeatureServerSpecification extends Specification
  with RestApiDriver {

  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  implicit val currentApp = GuiceApplicationBuilder().build()
  implicit val currentMat = currentApp.materializer

  //override decorate so we can inject 'include' en 'sequential' arguments
  override def decorate(is: SpecStructure, env: Env) = {
    val dec = super.decorate(is, env)
    dec.setArguments(dec.arguments <| args(sequential = true))
  }

  override def map(fs: => Fragments) = fs ^ step(currentApp.stop())

}

abstract class InDatabaseSpecification extends FeatureServerSpecification {

  override def map(fs: => Fragments) =
    super.map(step(makeDatabase(testDbName)) ^
      fs ^
      step(dropDatabase(testDbName)))

}

abstract class InCollectionSpecification extends FeatureServerSpecification {

  //TODO -- can we make it so that we don't have to prune crs?
  private val coordinateFilter = (__ \ "geometry" \ "coordinates").json.prune
    .composeWith((__ \ "geometry" \ "bbox").json.prune)
    .composeWith((__ \ "geometry" \ "crs").json.prune)

  //utility and matcher definitions

  override def map(fs: => Fragments) = super.map(
    step(makeDatabase(testDbName)) ^
      step(makeCollection(testDbName, testColName)) ^
      fs ^
      step(dropDatabase(testDbName))
  )

  def pruneSpecialProperties(js: JsValue): JsValue = {
    val tr: Reads[JsObject] = (__ \ "_id").json.prune andThen (__ \ "_mc").json.prune andThen (__ \ "_bbox").json.prune
    js match {
      case js: JsObject => js.transform(tr).asOpt.getOrElse(JsNull)
      case js: JsArray  => JsArray(js.value.map(el => pruneSpecialProperties(el)))
      case _            => sys.error(s"Can't prune value which isn't JSON object or array: ${Json.stringify(js)}")
    }
  }

  def matchFeaturesInJson(expected: JsArray): Matcher[JsValue] = (
    (js: JsValue) => {
      val receivedFeatureArray = (js \ "features").as[JsValue]
      (receivedFeatureArray must beFeatures(expected)).isSuccess
    }, s"Featurecollection Json doesn't contain expected features (${Json.stringify(expected)})"
  )

  def matchFeaturesInCsv(expectedColumnHeader: String): Matcher[Seq[String]] = (
    (received: Seq[String]) => {
      val lines = received.flatMap(l => received(0).split("\n")).map(_.trim)
      val header = lines(0)
      header == expectedColumnHeader
    }, "Featurecollection CSV doesn't contain expected columns"
  )

  def matchTotalInJson(expectedTotal: Int): Matcher[JsValue] = (
    (recJs: JsValue) => {
      val optTotalReceived = (recJs \ "total").asOpt[Int]
      (optTotalReceived must beSome(expectedTotal)).isSuccess
    }, s"FeatureCollection Json doesn't have expected value for total field ($expectedTotal)."
  )

  def filterCoordinates(in: JsValue): JsValue = {
    in.transform(coordinateFilter) match {
      case JsSuccess(tr, _) => tr
      case _                => in
    }
  }

  //TODO the verify() function is now very convoluted and unreadable.
  def verify(rec: JsValue, expected: JsArray, sortMatters: Boolean = false) = rec match {
    case jsv: JsArray =>
      val received = jsv.value.map(pruneSpecialProperties _ andThen filterCoordinates _)
      val expFiltered = expected.value.map(filterCoordinates)
      val ok =
        if (sortMatters) {
          received.equals(expFiltered)
        } else {
          received.toSet.equals(expFiltered.toSet)
        } //toSet so test in order independent
      val msg = if (!ok) {
        // put this in debug logging!!!
        //        println("___________________________________________________________________________________________________________")
        //        println(s"REC: ")
        //        received.foreach(println)
        //        println(s"EXP:  ")
        //        expFiltered.foreach(println)
        //        println("___________________________________________________________________________________________________________")
        (for (f <- received if !expFiltered.contains(f)) yield {
          f
        }).headOption.
          map(f => s" e.g. ${Json.stringify(f)}\nnot found among expected features. Example of expected:\n" +
            s"${expFiltered.headOption.getOrElse("<None expected.>")}")
          .getOrElse("<Missing object in received>")
      } else {
        "Received array matches expected"
      }
      (ok, msg)
    case _ => (false, "Did not receive array")
  }

  case class beFeatures(expected: JsArray) extends Matcher[JsValue] {
    def apply[J <: JsValue](r: Expectable[J]) = {
      lazy val (succ, msg) = verify(r.value, expected)
      result(
        succ,
        msg,
        msg,
        r
      )
    }
  }

  case class beSomeFeatures(expected: JsArray, sorted: Boolean = false) extends Matcher[Option[JsValue]] {
    def apply[J <: Option[JsValue]](r: Expectable[J]) = {
      lazy val (succ, msg) = if (!r.value.isDefined) {
        (false, "Expected Some(<features>), received None")
      } else {
        verify(r.value.get, expected)
      }
      result(
        succ,
        "received Some(<features>) with features matching expected",
        msg,
        r
      )
    }
  }

}

