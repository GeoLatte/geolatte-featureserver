package persistence.querylang

import org.specs2.mutable.Specification
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

/**
 * Created by Karel Maesen, Geovise BVBA on 30/03/17.
 */
class ProjectionParserSpec extends Specification {

  "The ProjectionParser.mkProjection( List[JsPath]) " should {

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

      val projection = ProjectionParser.mkProjection(pathList).get

      val obj = jsObj.as(projection)

      obj must_== Json.parse(projectedJsonText).as[JsObject]

    }

    "be robust w.r.t. to redundant path elements " in {
      val redundantPaths = __ \ "properties" \ "entity" \ "ident8" :: pathList

      val projection = ProjectionParser.mkProjection(redundantPaths).get

      val obj = jsObj.as(projection)

      obj must_== Json.parse(projectedJsonText).as[JsObject]

    }

    "Projection puts JsNull for non-existent (empty) paths" in {

      val withNonExistentPath = pathList :+ (__ \ "properties" \ "entity" \ "doesntexist")

      val projection = ProjectionParser.mkProjection(withNonExistentPath).get

      val obj = jsObj.asOpt(projection)

      val expected = Some(Json.obj("properties" ->
        Json.obj("entity" ->
          Json.obj("doesntexist" -> JsNull))).deepMerge(Json.parse(projectedJsonText).as[JsObject]))

      expected must_== obj

    }

  }

  "ProjectionParser  " should {

    val jsonText =
      """
          | { "a1" : 1, "a2" : { "b1" : [ { "ca" : 2, "cb" : 3}, { "ca" : 4, "cb" : 5 }  ], "b2" : 7 }, "a3" : 8 }
        """.stripMargin

    val json = Json.parse(jsonText).as[JsObject]

    " succesfully parse projection expression: a1, a2.b1[ca] " in {
      val projection = ProjectionParser.parse("a1, a2.b1[ca]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj("a1" -> 1, "a2" -> Json.obj("b1" -> Json.arr(Json.obj("ca" -> 2), Json.obj("ca" -> 4))))

      expected must_== obj
    }

  }

}