package integration

import featureserver.json.Gen
import featureserver.json.Gen._
import play.api.libs.json._
import play.api.http.Status._
import org.geolatte.geom.Envelope
import play.api.test.Helpers
import akka.util.Timeout

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/6/13
 */
class ViewDefsAPISpec extends InCollectionSpecification {

  def is = s2"""

     The ViewDefs /put should:
       Return 404 when the collection does not exist                              $e1
       Return CREATED when the view did not yet exist                             $e2
        and create the view                                                       $e3
        and response has a LOCATION header                                        $e4

       Return OK when the view already existed                                    $e5
        and replace the view                                                      $e6
       Allow empty projection parameters                                          $e7 
                                                                                  
  """

  //import default values
  import UtilityMethods._

  val viewName = "view-1"
  val jsInViewDef = Json.obj("query" -> JsString("id = 1"), "projection" -> Json.arr("foo", "bar"))
  val jsInViewDef2 = Json.obj("query" -> JsString("id = 2"), "projection" -> Json.arr("foo", "bar"))
  val InViewDefNoProjection = Json.obj("query" -> JsString("foo = 'bar'"))
  val jsOutViewDef = Json.obj("name" -> viewName, "query" -> JsString("id = 1"), "projection" -> Json.arr("foo", "bar"))
  val jsOutViewDef2 = Json.obj("name" -> viewName, "query" -> JsString("id = 2"), "projection" -> Json.arr("foo", "bar"))
  val OutViewDefNoProjection = InViewDefNoProjection ++ Json.obj("name" -> "view-2", "projection" -> Json.arr())

  def e1 = putView(testDbName, "NonExistingCollection", viewName, jsInViewDef) applyMatcher (_.status must equalTo(NOT_FOUND))

  def e2 = putView(testDbName, testColName, viewName, jsInViewDef) applyMatcher (_.status must equalTo(CREATED))

  def e3 = getViews(testDbName, testColName) applyMatcher (res => pruneUrl(res.responseBody) must beSome(Json.arr(jsOutViewDef)))

  def e4 = {
    deleteView(testDbName, testColName, viewName)
    putView(testDbName, testColName, viewName, jsInViewDef) applyMatcher (res =>
      Helpers.headers(res).get("Location") must beSome(s"/api/databases/$testDbName/$testColName/views/view-1"))
  }

  def e5 = putView(testDbName, testColName, viewName, jsInViewDef2) applyMatcher (_.status must equalTo(OK))

  def e6 = getViews(testDbName, testColName) applyMatcher (res => pruneUrl(res.responseBody) must beSome(Json.arr(jsOutViewDef2)))

  def e7 = {
    val create = putView(testDbName, testColName, "view-2", InViewDefNoProjection)
    val get = getView(testDbName, testColName, "view-2")
    (create.status must equalTo(CREATED)) and (pruneUrl(get.responseBody) must beSome(OutViewDefNoProjection))
  }

  val pruneUrlReads: Reads[JsObject] = (__ \ "url").json.prune
  def pruneUrl(inJs: JsValue): JsValue = inJs match {
    case js: JsObject => js.transform(pruneUrlReads).asOpt.get
    case js: JsArray => JsArray(js.value.map(pruneUrl))
    case _ => JsNull
  }

  def pruneUrl(inOpt: Option[JsValue]): Option[JsValue] = inOpt.map(js => pruneUrl(js))

}
