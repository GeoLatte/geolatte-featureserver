package utilities

import play.api.libs.json._
import play.api.libs.json.Reads._ //note that this imports the required object reducer
import play.api.libs.functional.syntax._ //note tha this import is required vor the functional composition of Reads

import play.api.data.validation.ValidationError

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
   * @param jsObject
   */
  def flatten(jsObject: JsObject) : Seq[(String, JsValue)]=  {

    def join(path: String, key: String) : String =
      new StringBuilder(path).append(".").append(key).toString()

    def prependPath(path: String, kv : (String, JsValue)) : (String, JsValue) = kv match {
      case (k,v) => (join(path,k), v)
    }

    def flattenAcc(jsObject: JsObject, buffer:ListBuffer[(String, JsValue)]): ListBuffer[(String, JsValue)] = {
      jsObject.fields.foreach {
        case (k, v: JsObject) => buffer.appendAll( flattenAcc(v, ListBuffer()).map( prependPath(k, _) )  )
        case (k, v: JsArray) => Unit
        case (k, v: JsValue) => buffer.append((k, v))
      }
      buffer
    }

    flattenAcc(jsObject, ListBuffer()).toSeq
  }


  def mkProjection(paths: List[JsPath]): Option[Reads[JsObject]] =
    if (paths.isEmpty) None
    else {
      val r = paths.foldLeft[Reads[JsObject]](NoObjReads) {
        (r1, path) => (r1 and path.json.pickBranch.orElse(path.json.put(JsNull))) reduce
      }
      Some(r)
    }



  /**
   * Converts a Json validation error sequence for a Feature into a single error message String.
   * @param errors JsonValidation errors
   * @return
   */
  implicit def JsValidationErrors2String(errors: Seq[(JsPath, Seq[ValidationError])]): String = {
    errors map {
      case (jspath, valerrors) => jspath + " :" + valerrors.map(ve => ve.message).mkString("; ")
    } mkString "\n"
  }

  object NoObjReads extends Reads[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] = JsSuccess(Json.obj())
  }

}
