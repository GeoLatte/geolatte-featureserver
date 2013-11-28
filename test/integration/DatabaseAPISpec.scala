package integration

import org.specs2.mutable.Specification
import play.api.test.FakeApplication
import play.api.test.Helpers._
import scala.Some
import play.api.libs.json._
import play.api.test.FakeApplication
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import scala.Some

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
        val retval = result.responseBody.get

        result.status must equalTo(OK) and
        ( contentType(result.wrappedResult) must equalTo(Some("vnd.geolatte-featureserver+json"))) and
        ( retval must beAnInstanceOf[JsArray] )
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
          case Some(jArray: JsArray) => jArray.value.filter(jsv => (jsv \ "name").as[String] == testDbName).headOption
          case _ => None
        }


        val dbReprAfterDrop = getDatabasesAfterDrop.responseBody match {
          case Some(jArray: JsArray) => jArray.value.filter(jsv => (jsv \ "name").as[String] == testDbName).headOption
          case _ => None
        }

        val retValRB = getResultDb.responseBody.get

        createResult.status must equalTo(CREATED) and
          (dbRepr must beSome) and
          (getResultDb.status must equalTo(OK) ) and
          (retValRB must beAnInstanceOf[JsArray] ) and
          (dropResult.status must equalTo(OK)) and
          (dbReprAfterDrop must beNone) and
          (getResultDBAfterDrop.status must equalTo(NOT_FOUND) )
      }
     }

    "On a PUT/DELETE of a collection, the collection is created/deleted" in {
      RestApiDriver.onDatabase(testDbName, fakeApplication) {
        //the inbound collection metadata
        val inMd = Json.obj(
          "index-level" -> 4,
          "extent" -> Json.obj(
            "crs" -> 4326,
            "envelope" -> Json.arr(0.0, 0.0, 90.0, 90.0)
          )
        )

        // the outbound collection metadata
        val outMd = inMd + ("collection" -> JsString(testColName)) + ("count" -> JsNumber(0))


        //create collection
        val createdColl = RestApiDriver.makeCollection(testDbName, testColName, inMd)
        val checkCreatedCol = createdColl match {
          case ResultCheck(status, None) if status == CREATED => success
          case _ => failure
        }

        val checkGetCollAfterCreation = RestApiDriver.getCollection(testDbName, testColName) match {
          case ResultCheck(status, Some(body)) if (status == OK && body == outMd) => success
          case _ => failure
        }

        //TODO -- still check the response of /database/:db GET
        // val getDb = RestApiDriver.getDatabase(testDbName)

        checkCreatedCol and checkGetCollAfterCreation
      }
    }
  }
}
