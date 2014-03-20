package utilities

import play.api.libs.json.JsPath
import play.api.data.validation.ValidationError

/**
 * Helpers for working the Json lib 
 * 
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/4/13
 */
object JsonHelper {

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
  
}
