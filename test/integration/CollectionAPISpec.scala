package integration

import play.api.libs.json._
import play.api.http.Status._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
class CollectionAPISpec extends InDatabaseSpecification {

  def is = s2"""

      The Collection API should:                                                            ${section("mongodb","postgresql")}
        return CREATED on PUT of a collection                                               $e1
        return CONFLICT on attempt to POST to same collection name                          $e2
        return an array of collections on GET to database                                   $e6
        Out Metadata contains collection and count fields                                   $e3
        return OK on attempt to delete the collection                                       $e4
        return emtpy array on GET to database                                               $e8
        return 404 on attempt to GET a collection which does not exist                      $e5
        return OK on attempt to DELETE a collection which does not exist                    $e7
        return 404 on attempt to create a collection on a non-existing database             $e9
                                                                                            ${section("mongodb","postgresql")}
    """

  import RestApiDriver._
  import UtilityMethods._

  def e1 = makeCollection(testDbName, testColName, inMetadata)
    .applyMatcher( _.status must equalTo(CREATED))

  def e2 = makeCollection(testDbName, testColName, inMetadata)
      .applyMatcher( _.status must equalTo(CONFLICT))

  def e6 = getDatabase(testDbName).applyMatcher(res =>
    (res.status must equalTo(OK)) and (res.responseBody must beSome(equalTo(outArray)))
  )

  def e3 = getCollection(testDbName, testColName).applyMatcher(res => {
    (res.status must equalTo(OK)) and (res.responseBody must beSome(equalTo(outMetadata)))
  })

  def e4 = deleteCollection(testDbName, testColName).applyMatcher(_.status must equalTo(OK))

  def e5 = getCollection(testDbName, testColName).applyMatcher(_.status must equalTo(NOT_FOUND))

  def e7 = deleteCollection(testDbName, testColName).applyMatcher(_.status must equalTo(OK))

  def e8 = getDatabase(testDbName).applyMatcher(res =>
    (res.status must equalTo(OK)) and (res.responseBody must beSome(equalTo(Json.arr())))
  )

  def e9 = makeCollection("doesntexist", testColName, inMetadata)
    .applyMatcher( _.status must equalTo(NOT_FOUND))

  // the inbound metadata
  val inMetadata = Json.obj(
    "index-level" -> 4,
    "extent" -> Json.obj(
      "crs" -> 4326,
      "envelope" -> Json.arr(0.0, 0.0, 90.0, 90.0)
    )
  )

  // the expected outbound collection metadata
  val outMetadata = inMetadata + ("collection" -> JsString(testColName)) + ("count" -> JsNumber(0))

  val outArray = Json.arr(
    Json.obj("name" -> "xfstestcoll", "url" -> "/api/databases/xfstestdb/xfstestcoll")
  )

}
