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
    //Generators for data
    val prop = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3))
    val geom = Gen.lineString(3)
    val feature = Gen.geoJsonFeature(Gen.id, geom, prop)



    val fakeApplication = FakeApplication()

    "On a GET of a collection the collection metadata should be returned" in {
      import RestApiDriver._
      val dataStr = List.fill(100)(0).map(_ => Json.stringify(feature.sample.get)) mkString "\n"
      val rawBody = dataStr.getBytes("UTF-8")
      onCollection(testDbName, testColName, fakeApplication) {
        val result = RestApiDriver.getCollection(testDbName, testColName)
        val js = result.responseBody

        (result.status must equalTo(OK)) and
          ((js \ "count").as[Int] must equalTo(0)) and
          ((js \ "extent" \ "crs").as[Int] must equalTo(defaultExtent.getCrsId.getCode)) and
          ((js \ "collection").as[String] must equalTo(testColName)) and
          ((js \ "index-level").as[Int] must equalTo(defaultIndexLevel))
      }
    }

    "On a GET db/col the collection metadata should be returned" in {
      import RestApiDriver._
      val dataStr = List.fill(100)(0).map(_ => Json.stringify(feature.sample.get)) mkString "\n"
      val rawBody = dataStr.getBytes("UTF-8")
      withData(testDbName, testColName, rawBody, fakeApplication) {
        val result = RestApiDriver.getCollection(testDbName, testColName)
        val js = result.responseBody


        println("OUTPUT: " + js)

        (result.status must equalTo(OK)) and
          ((js \ "count").as[Int] must equalTo(100)) and
          ((js \ "extent" \ "crs").as[Int] must equalTo(defaultExtent.getCrsId.getCode)) and
          ((js \ "collection").as[String] must equalTo(testColName)) and
          ((js \ "index-level").as[Int] must equalTo(defaultIndexLevel))
      }

    }

  }


}
