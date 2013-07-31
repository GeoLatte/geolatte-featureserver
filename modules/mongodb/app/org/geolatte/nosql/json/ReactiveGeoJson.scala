package org.geolatte.nosql.json

import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee._
import play.extras.iteratees._
import org.geolatte.common.Feature
import play.api.libs.json.JsObject
import play.api.libs.json.JsNumber
import play.api.libs.iteratee.Input.{El, Empty, EOF}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {


  import JsonIteratees._
  import JsonEnumeratees._
  import GeometryReaders._
  import scala.language.reflectiveCalls

  // case class that we will fold the result of the parsing into
  case class Errors(msg: List[String] = Nil)

  // Map function that ignores the input, and returns an identity function to leave errors alone
  def ignore[A]: A => Errors => Errors = (_) => identity[Errors]


  def featureCollectionIteratee[A](featureValueParser: Iteratee[Option[Feature], A]) = {

    def toCrs(js: JsNumber) = {
      val epsg = js.value.toInt
      try {
        CrsId.valueOf(epsg)
      } catch {
        case ex: Throwable => Errors(List("Can't read CRS"))
      }
    }

    def parseFeature(crs: CrsId): Enumeratee[JsObject, Option[Feature]] = Enumeratee.map {
       implicit val reader = FeatureReads(crs)
       obj =>  obj.validate[Feature].asOpt
     }


    val valueHandlers = (key: String) => key match {
      case "crs" => jsNumber.map(n => toCrs(n))
      case "features" => (jsArray(jsValues(jsSimpleObject)) compose parseFeature(CrsId.UNDEFINED)) &>> featureValueParser
      case k: String => jsSimpleObject.map(v => (k, v))
    }

    jsObject(valueHandlers)

  }

}
