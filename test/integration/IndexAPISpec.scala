package integration

import play.api.libs.json._

/**
 * Created by Karel Maesen, Geovise BVBA on 19/02/15.
 */
class IndexAPISpec extends InCollectionSpecification {


  def is = s2"""
                                                                                    ${section("mongodb", "postgresql")}
      The PUT index should:
        return 404 when the collection does not exist                               $e1
        return 400 (BAD REQUEST) when index data is invalid                         $e2
        return OK when the collection does exist, and indexdata is valid            $e3


  """

  import RestApiDriver._
  import UtilityMethods._

  val indexDef = Json.obj(
    "path" -> "a.b" ,
    "type" -> "text"
  )


  def e1 = putIndex(testDbName, "NonExistingCollection", "my_idx", indexDef) applyMatcher( _.status must equalTo(NOT_FOUND))

  def e2 = putIndex(testDbName, testColName, "my_idx", Json.obj("bla" -> 2)) applyMatcher( _.status must equalTo(BAD_REQUEST))

  def e3 = putIndex(testDbName, testColName, "my_idx", indexDef) applyMatcher( _.status must equalTo(OK))

}
