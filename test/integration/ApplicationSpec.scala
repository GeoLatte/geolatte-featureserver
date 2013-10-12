package integration

import org.specs2.mutable._

import play.api.test._
import play.api.test.Helpers._
import play.api.libs.ws.WS
import play.api.libs.json._
import UtilityMethods._

/**
 * API Spec
 *
 */
//TODO -- run tests with an embedded Mongo instance
class ApplicationSpec extends Specification {

  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  "Application" should {

    "send 404 on a bad request" in new WithServer {
      await(WS.url("http://localhost:" + port + "/foo").get).status must equalTo(NOT_FOUND)
    }

    "redirect to the index page" in {
      running(FakeApplication()) {
        val home = route(FakeRequest(GET, "/")).get

        status(home) must equalTo(SEE_OTHER)
        header("Location", home) must beSome("/index.html")
      }
    }

    "send an array of databases on /api/databases" in {
      running(FakeApplication()) {

        val result = RestApiDriver.getDatabases

        result.status must equalTo(OK) and
        ( contentType(result.wrappedResult) must equalTo(Some("vnd.geolatte-featureserver+json"))) and
        ( result.responseBody must beAnInstanceOf[JsArray] )
      }
    }

    "On a PUT/DELETE of a database, the database is created/deleted" in {
      running(FakeApplication()) {

        val createResult = RestApiDriver.makeDatabase(testDbName)
        val getDatabases = RestApiDriver.getDatabases
        val getResultDb = RestApiDriver.getDatabase(testDbName)
        val dropResult = RestApiDriver.dropDatabse(testDbName)
        val getDatabasesAfterDrop = RestApiDriver.getDatabases
        val getResultDBAfterDrop = RestApiDriver.getDatabase(testDbName)


        val dbRepr = getDatabases.responseBody match {
          case jArray: JsArray => jArray.value.filter(jsv => (jsv \ "name").as[String] == testDbName).headOption
          case _ => None
        }


        val dbReprAfterDrop = getDatabasesAfterDrop.responseBody match {
          case jArray: JsArray => jArray.value.filter(jsv => (jsv \ "name").as[String] == testDbName).headOption
          case _ => None
        }

        createResult.status must equalTo(CREATED) and
          (dbRepr must beSome) and
          (getResultDb.status must equalTo(OK) ) and
          (getResultDb.responseBody must beAnInstanceOf[JsArray] ) and
          (dropResult.status must equalTo(OK)) and
          (dbReprAfterDrop must beNone) and
          (getResultDBAfterDrop.status must equalTo(NOT_FOUND) )

      }
    }





  }
}