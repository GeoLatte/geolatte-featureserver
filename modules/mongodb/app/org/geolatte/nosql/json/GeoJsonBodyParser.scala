package org.geolatte.nosql.json



import play.extras.iteratees._
import play.api.libs.iteratee._
import org.geolatte.common.Feature
import org.geolatte.geom._
import org.geolatte.geom.DimensionalFlag._
import play.api.libs.json._
import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee.Input.Empty
import scala.language.reflectiveCalls
import JsonBodyParser._
import JsonIteratees._
import JsonEnumeratees._
import scala.concurrent.ExecutionContext.Implicits.global

object GeoJsonBodyParser {




//  val GeoJsonFeature: Iteratee[Array[Char], Feature]

//  def importItem: Enumeratee[Either[Errors, Feature], String]



//  val valueHandler:Iteratee[Array[Char], Feature] =  null


  def mkPoint(c: List[Double]) = c match {
    case List(x,y) => org.geolatte.geom.Points.create2D(x,y)
    case List(x,y,z)  => org.geolatte.geom.Points.create3D(x,y,z)
  }

  val coordinateValueHandler = (jsArray(jsValues(jsNumber)) compose Enumeratee.map( jsn => jsn.value.doubleValue())) &>> Iteratee.getChunks[Double].map( l => mkPoint(l) )

  val coordinateArrayValueHandler = jsArray(jsValues(coordinateValueHandler)) &>> Iteratee.getChunks[Point]

  val arrayHelpValueHandler = jsArray(jsValues(jsValue)) &>> Iteratee.getChunks[JsValue]




//  def geometryElementValueParsers(key: String) = key match {
//    case "type" => jsString
//    case "coordinates" =>
//  }
//
//  def jsGeometry[V](valueParsers: (String, Iteratee[Array[Char], V])*): Enumeratee[Array[Char], V] = {
//      jsObject((key: String) => Map(valueParsers:_*).get(key).getOrElse(Error("Unexpected key found in JSON: " + key, Empty)))
//    }

  val featureValueHandler = (jsObject("properties" -> jsSimpleObject, "features" ->jsString) compose Enumeratee.map(obj => JsObject(List("properties" -> obj)))) &>>  Iteratee.getChunks[JsObject]


  val arrayValueHandler  = jsArray( jsValues (jsNumber) )  &>>  Iteratee.getChunks[JsNumber]

  val valueHandler =   jsObject("features" -> arrayValueHandler )  &>> Iteratee.fold( List[JsNumber]() )( (e,f) => f ++ e)

  val bodyparser = parser (
    valueHandler
  )




}