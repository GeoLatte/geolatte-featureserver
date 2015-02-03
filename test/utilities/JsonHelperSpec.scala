package utilities

import org.specs2.mutable.Specification
import play.api.libs.functional.Reducer
import play.api.libs.json._


/**
 * Created by Karel Maesen, Geovise BVBA on 21/11/14.
 */
class JsonHelperSpec extends Specification {

  "The JsonHelper.flatten " should {

    val obj = Json.obj(
      "id" -> 1,
      "att" -> "value",
      "nested" -> Json.obj("foo" -> "bar"),
      "nested2" -> Json.obj("foo" -> Json.obj("bar" -> 42)),
      "arr" -> Json.arr(1, 2, 3, 4)
    )


    "strip contain nested keys, but not arrays " in {
      JsonHelper.flatten(obj) must containTheSameElementsAs(Seq(
        ("id", JsNumber(1)), ("att", JsString("value")), ("nested.foo", JsString("bar")), ("nested2.foo.bar", JsNumber(42))
      ))
    }

  }


   "The JsonHelper.mkProjection( List[JsPath]) " should {

    val jsonText =
      """
        |{"id":"123747","geometry":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"type":"Feature","properties":{"entity":{"id":123747.0,"langsGewestweg":true,"ident8":"R0010751","opschrift":0.0,"afstand":5.0,"zijdeVanDeRijweg":"RECHTS","locatie":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"binaireData":{"kaartvoorstellingkleingeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAANUlEQVR42mNggIH//32AuAGIA4CYhQFJYjUQ/0fCqyEKIDr+Y8EBDFCjsEk2ENCJ1048rgUADfltxmBALPMAAAAASUVORK5CYII=","md5":"NdkrvxQrcszB7vQWjcd/Kg=="},"kaartvoorstellingklein":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAAMklEQVR42mNgQAAfIG4A4gAgZkESZ1gNxP+R8GqYAh80CRgGmQA2CptkA0GdeO3E6VoA+T0cGBVUSPoAAAAASUVORK5CYII=","md5":"WuQL2tBI1Wslm3+8GwyzAw=="}}}},"_bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735]}
      """.stripMargin

    val projectedJsonText =
      """
        |{"geometry":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"type":"Feature","properties":{"entity":{"id" : 123747.0, "ident8":"R0010751"}}}
      """.stripMargin

    val jsObj = Json.parse(jsonText).as[JsObject]

    val pathList: List[JsPath] = List(__ \ "geometry", __ \ "type", __ \ "properties" \ "entity" \ "id", __ \ "properties" \ "entity" \ "ident8")


     "create a Reads[Object] that strips all non-mentioned properties from the json" in {

       val projection = JsonHelper.mkProjection(pathList)

       val obj = jsObj.as(projection)

       obj must_== Json.parse(projectedJsonText).as[JsObject]

     }

     "be robust w.r.t. to redundant path elements " in {
       val redundantPaths = __ \ "properties" \ "entity" \ "ident8" :: pathList

       val projection = JsonHelper.mkProjection(redundantPaths)

       val obj = jsObj.as(projection)

       obj must_== Json.parse(projectedJsonText).as[JsObject]

     }

     "fails on non-existent (empty) paths" in {

       val withNonExistentPath = __ \ "properties" \ "entity" \ "doesntexist" :: pathList

       val projection = JsonHelper.mkProjection(withNonExistentPath)

       val obj = jsObj.asOpt(projection)

       obj must_== None

     }

  }
}
