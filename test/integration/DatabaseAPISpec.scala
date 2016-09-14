package integration

import play.api.libs.json._
import play.api.mvc.AnyContentAsEmpty

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/26/13
 */
class DatabaseAPISpec extends FeatureServerSpecification {

  //These specifications need to be sequential (we test for objects created in previous steps/examples
  def is = s2"""



    The /api/databases should return
      an array of databases                             ${e1}
      with content-type                                 ${e2("application/vnd.geolatte-featureserver+json")}
      CREATED on PUT of a database                      ${e3}
      CONFLICT on attempt to create twice               ${e4}
      array containing the name of db after create      ${e5}
      the db metadata on GET of db                      ${e6}
      OK on deleting the database                       ${e7}
      OK when deleting again (DELETE is Idempotent)     ${e7}
      array without name of db, after drop              ${e8}
                                                        ${step(cleanup)}

  """

  import integration.UtilityMethods._

  def e1 = getDatabases.applyMatcher { it => it.status must equalTo(OK) and (it.responseBody must beSome(beAnInstanceOf[JsArray])) }

  def e2(expected: String) = getDatabases applyMatcher { it => contentType(it.wrappedResult) must beSome(expected) }

  def e3 = makeDatabase(testDbName) applyMatcher (_.status must equalTo(CREATED))

  def e4 = makeDatabase(testDbName) applyMatcher (_.status must equalTo(CONFLICT))

  def e5 = getDatabases.applyMatcher(testResponseContains(testDbName, 1))

  def e6 = pending //TODO see CollectionAPISpec#e6 (move that to here, or delete this)

  def e7 = dropDatabase(testDbName) applyMatcher (_.status must equalTo(OK))

  def e8 = getDatabases.applyMatcher(testResponseContains(testDbName, 0))

  def cleanup = { dropDatabase(testDbName); success }

  def testResponseContains(dbName: String, numTimes: Int) = (res: FakeRequestResult[Nothing, AnyContentAsEmpty.type, JsValue]) => {
    val jsArrOpt: Option[JsArray] = res.responseBody
    val test = (__ \ "name").read[String]
    //TODO -- test for presence of correct URL
    val filtered = jsArrOpt.map(js => js.value.filter(value => value.validate(test).asOpt.getOrElse("") == testDbName))
    (filtered must beSome((sq: Seq[JsValue]) => sq must have size numTimes)) and (res.status must equalTo(OK))
  }

}
