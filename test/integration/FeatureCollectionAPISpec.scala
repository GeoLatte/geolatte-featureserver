package integration

import org.specs2.mutable.Specification
import play.api.test.FakeApplication
import nosql.json.Gen
import play.api.libs.json._
import play.api.http.Status._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class FeatureCollectionAPISpec extends Specification {

  "The application" should {
    val testDbName = "xfstestdb"
    val testColName = "xfstestcoll"

    //import default values
    import UtilityMethods._
    import RestApiDriver._

    //Generators for data
    val prop = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3))
    val geom = Gen.lineString(3)
    val feature = Gen.geoJsonFeature(Gen.id, geom, prop)
    val fakeApplication = FakeApplication()

    "On a GET db/col the collection metadata should contain the number of elements" in {
      val dataStr = List.fill(100)(0).map(_ => Json.stringify(feature.sample.get)) mkString "\n"
      val rawBody = dataStr.getBytes("UTF-8")
      withData(testDbName, testColName, rawBody, fakeApplication) {
        val result = RestApiDriver.getCollection(testDbName, testColName)
        val js = result.responseBody

        (result.status must equalTo(OK)) and
          ((js \ "count").as[Int] must equalTo(100)) and
          ((js \ "extent" \ "crs").as[Int] must equalTo(defaultExtent.getCrsId.getCode)) and
          ((js \ "collection").as[String] must equalTo(testColName)) and
          ((js \ "index-level").as[Int] must equalTo(defaultIndexLevel))
      }
    }

    "On a download of db/col the result should contain the number of elements" in {
      val dataStr = List.fill(10)(0).map(_ => Json.stringify(feature.sample.get)) mkString "\n"
      val rawBody = dataStr.getBytes("UTF-8")
      withData(testDbName, testColName, rawBody, fakeApplication) {
        val result = RestApiDriver.getDownload(testDbName, testColName)

        //if result isn't an array then turn it into an empty JsArray
        val js : JsArray = result.responseBody match {
          case jsArr : JsArray => jsArr
          case _ => JsArray()
        }

        (result.status must equalTo(OK)) and
          (js.value.size must equalTo(10))
      }
    }

  }
}
