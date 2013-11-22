package integration

import org.specs2.mutable.Specification
import play.api.test.FakeApplication
import play.api.test.Helpers._
import scala.Some
import play.api.libs.json.JsArray

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/26/13
 */
class DatabaseAPISpec extends Specification {

  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  "Application" should {

    val fakeApplication = FakeApplication()

    "send an array of databases on /api/databases" in {
      running(fakeApplication) {

        val result = RestApiDriver.getDatabases

        result.status must equalTo(OK) and
        ( contentType(result.wrappedResult) must equalTo(Some("vnd.geolatte-featureserver+json"))) and
        ( result.responseBody must beAnInstanceOf[JsArray] )
      }
    }

    "On a PUT/DELETE of a database, the database is created/deleted" in {
      running(fakeApplication) {

        val createResult = RestApiDriver.makeDatabase(testDbName)
        val getDatabases = RestApiDriver.getDatabases
        val getResultDb = RestApiDriver.getDatabase(testDbName)
        val dropResult = RestApiDriver.dropDatabase(testDbName)
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
