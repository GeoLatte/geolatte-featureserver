package integration

import play.api.libs.json._
import play.api.http.Status._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
class CollectionAPISpec extends InDatabaseSpecification {

  def is = s2""" $sequential

      The Collection API should:
        return CREATED on POST of a collection                                              $e1
        return CONFLICT on attempt to POST to same collection name                          $e2
        Out Metadata contains collection and count fields                                   $e3

    """

  import RestApiDriver._
  import UtilityMethods._

  def e1 = RestApiDriver.makeCollection(testDbName, testColName, inMetadata)
    .applyMatcher( _.status must equalTo(CREATED))

  def e2 = RestApiDriver.makeCollection(testDbName, testColName, inMetadata)
      .applyMatcher( _.status must equalTo(CONFLICT))


  def e3 = RestApiDriver.getCollection(testDbName, testColName).applyMatcher(res => {
    (res.status must equalTo(OK)) and (res.responseBody must beSome(equalTo(outMetadata)))
  })

  // the inbound metadata
  val inMetadata = Json.obj(
    "index-level" -> 4,
    "extent" -> Json.obj(
      "crs" -> 4326,
      "envelope" -> Json.arr(0.0, 0.0, 90.0, 90.0)
    )
  )

  // the outbound collection metadata
  val outMetadata = inMetadata + ("collection" -> JsString(testColName)) + ("count" -> JsNumber(0))


}
