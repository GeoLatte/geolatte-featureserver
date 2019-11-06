package utilities

import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

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

    def join(path: String, key: String): String =
      new StringBuilder(path).append(".").append(key).toString()

    def prependPath(path: String, kv: (String, JsValue)): (String, JsValue) = kv match {
      case (k, v) => (join(path, k), v)
    }

    def flattenAcc(jsObject: JsObject, buffer: ListBuffer[(String, JsValue)]): ListBuffer[(String, JsValue)] = {
      jsObject.fields.foreach {
        case (k, v: JsObject) => buffer.appendAll(flattenAcc(v, ListBuffer()).map(prependPath(k, _)))
        case (k, v: JsArray) => Unit
        case (k, v: JsValue) => buffer.append((k, v))
      }
      buffer
    }

    flattenAcc(jsObject, ListBuffer()).toSeq
  }

  /**
   * Converts a Json validation error sequence for a Feature into a single error message String.
   * @param errors JsonValidation errors
   * @return
   */
  implicit def JsValidationErrors2String(errors: Seq[(JsPath, Seq[JsonValidationError])]): String = {
    errors map {
      case (jspath, valerrors) => jspath + " :" + valerrors.map(ve => ve.message).mkString("; ")
    } mkString "\n"
  }

}
