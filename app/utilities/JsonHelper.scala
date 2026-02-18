package utilities

import play.api.libs.json._

/**
 * Helpers for working the Json lib
 *
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/4/13
 */
object JsonHelper {

  /**
   * Flattens a Json object to a sequence of keys, values.
   *
   * {'a' : 1, 'b' : { 'c' , 2}} is flattened to Seq( ('a', 1), ('b.c', 2))
   * Arrays are filtered out
   *
   * @param jsObject object to flatten
   */
  def flatten(jsObject: JsObject): Seq[(String, JsValue)] = {

    def prependPath(path: String, kv: (String, JsValue)): (String, JsValue) = kv match {
      case (k, v) => (s"$path.$k", v)
    }

    def flattenRec(jsObject: JsObject): Seq[(String, JsValue)] =
      jsObject.fields.toSeq.flatMap {
        case (k, v: JsObject) => flattenRec(v).map(prependPath(k, _))
        case (_, _: JsArray)  => Seq.empty
        case (k, v)           => Seq((k, v))
      }

    flattenRec(jsObject)
  }

  /**
   * Converts a Json validation error sequence for a Feature into a single error message String.
   * @param errors JsonValidation errors
   * @return
   */
  def JsValidationErrors2String(errors: collection.Seq[(JsPath, collection.Seq[JsonValidationError])]): String = {
    errors.map {
      case (jspath, valerrors) => jspath.toJsonString + " :" + valerrors.map(ve => ve.message).mkString("; ")
    }.mkString("\n")
  }

}
