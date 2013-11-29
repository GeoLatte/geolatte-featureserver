package integration

import scala._
import org.specs2._
import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.WithApplication
import org.specs2.specification.Step


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/26/13
 */
class DatabaseAPISpec extends Specification {

  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  val running = new WithApplication {}

  //These specifications need to be sequential (we test for objects created in previous steps/examples
  def is =  s2""" $sequential

    The /api/databases should return
      an array of databases                             ${running(e1)}
      with content-type                                 ${running(e2("vnd.geolatte-featureserver+json"))}
      CREATED on PUT of a database                      ${running(e3)}
      CONFLICT on attempt to create twice               ${running(e4)}
      array containing the name of db after create      ${running(e5)}
      the db metadata on GET of db                      ${running(e6)}
      DELETED on deleting the database                  ${running(e7)}
      array without name of db, after drop              ${running(e8)}
                                                        ${Step(running(cleanup))}


  """

  import RestApiDriver._
  import UtilityMethods._

  def e1 =  getDatabases.applyMatcher{ it => it.status must equalTo(OK) and
        (it.responseBody must beSome(beAnInstanceOf[JsArray])) }

  def e2(expected: String) =  getDatabases applyMatcher { it => contentType(it.wrappedResult) must beSome(expected) }

  def e3 = makeDatabase(testDbName) applyMatcher( _.status must equalTo(CREATED))

  def e4 = makeDatabase(testDbName) applyMatcher( _.status must equalTo(CONFLICT))

  def e5 = getDatabases.applyMatcher( testResponseContains(testDbName, 1))

  def e6 = pending    //TODO - pending test

  def e7 = dropDatabase(testDbName) applyMatcher( _.status must equalTo(OK))

  def e8 = getDatabases.applyMatcher( testResponseContains(testDbName, 0))

  def cleanup = { RestApiDriver.dropDatabase(testDbName); success }

  def testResponseContains(dbName: String, numTimes: Int) = ( res: FakeRequestResult[JsValue, _]) => {
      val jsArrOpt : Option[JsArray] = res.responseBody
      val test = ( __ \ "name").read[String]
      //TODO -- test for presence of correct URL
      val filtered = jsArrOpt.map( js => js.value.filter( value => value.validate(test).asOpt.getOrElse("") == testDbName) )
      filtered must beSome( (sq:Seq[JsValue]) => sq must have size numTimes )
    }


 // TODO -- move this to its own specs


//    "On a PUT/DELETE of a collection, the collection is created/deleted" in {
//      RestApiDriver.onDatabase(testDbName, fakeApplication) {
//        //the inbound collection metadata
//        val inMd = Json.obj(
//          "index-level" -> 4,
//          "extent" -> Json.obj(
//            "crs" -> 4326,
//            "envelope" -> Json.arr(0.0, 0.0, 90.0, 90.0)
//          )
//        )
//
//        // the outbound collection metadata
//        val outMd = inMd + ("collection" -> JsString(testColName)) + ("count" -> JsNumber(0))
//
//
//        //create collection
//        val createdColl = RestApiDriver.makeCollection(testDbName, testColName, inMd)
//        val checkCreatedCol = createdColl match {
//          case ResultCheck(status, None) if status == CREATED => success
//          case _ => failure
//        }
//
//        val checkGetCollAfterCreation = RestApiDriver.getCollection(testDbName, testColName) match {
//          case ResultCheck(status, Some(body)) if (status == OK && body == outMd) => success
//          case _ => failure
//        }
//
//        //TODO -- still check the response of /database/:db GET
//        // val getDb = RestApiDriver.getDatabase(testDbName)
//
//        checkCreatedCol and checkGetCollAfterCreation
//      }
//    }
//  }
}
