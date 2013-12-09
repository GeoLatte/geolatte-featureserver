package integration

import nosql.json.Gen
import nosql.json.Gen._
import play.api.libs.json._
import play.api.http.Status._
import org.geolatte.geom.Envelope
import play.api.test.Helpers


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/6/13
 */
class ViewDefsAPISpec extends InCollectionSpecification {


  def is = s2""" $sequential

     The ViewDefs /put should:
       Return 404 when the collection does not exist                              $e1
       Return CREATED when the view did not yet exist                             $e2
        and create the view                                                       $e3
        and response has a LOCATION header                                        $e4
       Return OK when the view already existed                                    $e5
        and replace the view                                                      $e6
       Allow empty query and projection parameters                                $e7

  """

  //import default values
  import UtilityMethods._
  import RestApiDriver._

  val viewName = "view-1"
  val jsInViewDef = Json.obj("query" -> Json.obj("id" -> 1), "projection" -> Json.arr("foo", "bar"))
  val jsInViewDef2 = Json.obj("query" -> Json.obj("id" -> 2), "projection" -> Json.arr("foo", "bar"))
  val jsOutViewDef = Json.obj("name" -> viewName, "query" -> Json.obj("id" -> 1), "projection" -> Json.arr("foo", "bar"))
  val jsOutViewDef2 = Json.obj("name" -> viewName, "query" -> Json.obj("id" -> 2), "projection" -> Json.arr("foo", "bar"))



  def e1 = putView(testDbName, "NonExistingCollection", viewName, jsInViewDef) applyMatcher( _.status must equalTo(NOT_FOUND))

  def e2 = putView(testDbName, testColName, viewName, jsInViewDef) applyMatcher( _.status must equalTo(CREATED) )

  def e3 = getViews(testDbName, testColName) applyMatcher (_.responseBody.map(js => pruneUrl(js)) must beSome(Json.arr(jsOutViewDef)))

  def e4 = {
    deleteView(testDbName, testColName, viewName)
    putView(testDbName, testColName, viewName, jsInViewDef) applyMatcher( res =>
      Helpers.headers(res).get("Location") must beSome( "/api/databases/xfstestdb/xfstestcoll/views/view-1")
    )
  }

  def e5 = putView(testDbName, testColName, viewName, jsInViewDef2) applyMatcher( _.status must equalTo(OK))

  def e6 = getViews(testDbName, testColName) applyMatcher (_.responseBody.map(js => pruneUrl(js)) must beSome(Json.arr(jsOutViewDef2)))

  def e7 = putView(testDbName, testColName, "view-2", Json.obj()) applyMatcher( _.status must equalTo(CREATED))


  val pruneUrlReads :  Reads[JsObject] = (__ \ "url").json.prune
  def pruneUrl(inJs : JsValue) : JsValue = inJs match {
    case js : JsObject =>  js.transform(pruneUrlReads).asOpt.get
    case js : JsArray => JsArray(js.value.map( pruneUrl ))
    case _ => JsNull
  }

}
