package org.geolatte.nosql.json

import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee._
import play.extras.iteratees._
import org.geolatte.common.Feature
import play.api.libs.json.JsObject
import play.api.libs.json.JsNumber


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {


  import JsonIteratees._
  import JsonEnumeratees._
  import GeometryReaders._
  import JsonBodyParser._
  import scala.language.reflectiveCalls

  // case class that we will fold the result of the parsing into
  case class State(msg: String = "")

  val stateFolder = (r: State, s: State) => State(r.msg ++ "\n" ++ s.msg)

  def featureCollectionEnumeratee[A <: State](featureValueParser: Iteratee[Option[Feature], A]) = {

    def toCrs(js: JsNumber): CrsId = {
      val epsg = js.value.toInt
      try {
        CrsId.valueOf(epsg)
      } catch {
        case ex: Throwable => CrsId.UNDEFINED
      }
    }

    /**
     * A closure that hold a mutable CRS member and the ValueParsers for the JSonEnumeratee
     */
    def mkCrsSensitiveValueHandler = new {
      var crs: CrsId = CrsId.UNDEFINED
      val select = (key: String) => {
        key match {
          case "crs" => jsNumber.map(n => {
            crs = toCrs(n)
            State(s"CRS = $crs")
          })
          case "features" => (jsArray(jsValues(jsSimpleObject)) compose parseFeature(crs)) &>> featureValueParser
          case k: String => jsSimpleObject.map(v => State(s"Ignoring property $k"))
        }
      }
    }

    def parseFeature(crs: CrsId): Enumeratee[JsObject, Option[Feature]] = Enumeratee.map {
      implicit val reader = FeatureReads(crs)
      obj => obj.validate[Feature].asOpt
    }
    jsObject(mkCrsSensitiveValueHandler.select)
  }

  def writeFeatures : Iteratee[Option[Feature], State] = {
    val el = Enumeratee.mapInput[Option[Feature]] ( of => of match {
      case Input.El(Some(f)) =>
        println (f)
        Input.Empty
      case Input.El(None) => Input.El(State("Import Error"))
      case _ => Input.Empty
    })
    el &>> Iteratee.fold( State() )( stateFolder)
  }

  def bodyParser = parser (
   featureCollectionEnumeratee(writeFeatures)  &>> Iteratee.getChunks[State].map[State]( l => l.foldLeft(State())( stateFolder ))
  )

}
