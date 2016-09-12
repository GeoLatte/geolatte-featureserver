package integration

import play.api.libs.json._

/**
 * Created by Karel Maesen, Geovise BVBA on 19/02/15.
 */
class IndexAPISpec extends InCollectionSpecification {

  def is = s2"""

      The PUT on /index should:
        return 404 when the collection does not exist                               $e1
        return 400 (BAD REQUEST) when index data is invalid                         $e2
        return CREATED when the collection does exist, and indexdata is valid       $e3
        return CONFLICT when the index already exists                               $e4

      The GET on /indexes should:
        return 404 when the collection does not exist                               $e5
        return 200 and Json arraywhen db and collection exist                       $e6

      The GET on /indexes/<index-name> should:
        return 404 when the index does not exist                                    $e7
        return 200 and Json info when index does exist                              $e8

      The DELETE on /indexes/<index-name> should:
        return 200 to indicate success                                              $e9
          also when executed again (idem-potent)                                    $e9
          and then /indexes/<index-name> should return 404                          $e10
          and /indexes should return empty list                                     $e11

  """

  import RestApiDriver._
  import UtilityMethods._

  val indexDef = Json.obj(
    "path" -> "a.b",
    "type" -> "text"
  )

  def e1 = putIndex(testDbName, "NonExistingCollection", "my_idx", indexDef) applyMatcher (_.status must equalTo(NOT_FOUND))

  def e2 = putIndex(testDbName, testColName, "my_idx", Json.obj("bla" -> 2)) applyMatcher (_.status must equalTo(BAD_REQUEST))

  def e3 = putIndex(testDbName, testColName, "my_idx", indexDef) applyMatcher (_.status must equalTo(CREATED))

  def e4 = putIndex(testDbName, testColName, "my_idx", indexDef) applyMatcher (_.status must equalTo(CONFLICT))

  def e5 = getIndices(testDbName, "NonexistingCollection") applyMatcher (_.status must equalTo(NOT_FOUND))

  def e6 = getIndices(testDbName, testColName) applyMatcher (res =>
    (res.status must equalTo(OK)) and
      (res.responseBody must beSome(
        Json.arr(
          Json.obj("name" -> "my_idx", "url" -> "/api/databases/xfstestdb/xfstestcoll/indexes/my_idx")
        )
      )))

  def e7 = getIndex(testDbName, testColName, "doesntexist") applyMatcher (_.status must equalTo(NOT_FOUND))

  def e8 = getIndex(testDbName, testColName, "my_idx") applyMatcher { resp =>
    (resp.status must equalTo(OK)) and
      (resp.responseBody must beSome(Json.obj("name" -> "my_idx", "path" -> "a.b", "type" -> "text", "regex" -> false)))
  }

  def e9 = deleteIndex(testDbName, testColName, "my_idx") applyMatcher (_.status must equalTo(OK))

  def e10 = getIndex(testDbName, testColName, "my_idx") applyMatcher (_.status must equalTo(NOT_FOUND))

  def e11 = getIndices(testDbName, testColName) applyMatcher (res =>
    (res.status must equalTo(OK)) and
      (res.responseBody must beSome(
        Json.arr()
      )))

}
