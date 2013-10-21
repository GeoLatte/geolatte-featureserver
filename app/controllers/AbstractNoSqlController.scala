package controllers

import play.api.mvc.{RequestHeader, Controller, Result}
import nosql.Exceptions._
import utilities.SupportedMediaTypes
import config.ConfigurationValues.Format

import scala.language.implicitConversions
import play.api.Play
import play.Logger

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller {

  def toResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): Result = {
    (result, request) match {
      case (r : Jsonable, SupportedMediaTypes(Format.JSON, version)) => Ok(r.toJson).as(SupportedMediaTypes(Format.JSON, version))
      case (r : Csvable, SupportedMediaTypes(Format.CSV, version)) => Ok(r.toCsv).as(SupportedMediaTypes(Format.CSV, version))
      case _ => UnsupportedMediaType("No supported media type: " + request.acceptedTypes.mkString(";"))
    }
  }

  def commonExceptionHandler(db : String, col : String = "") : PartialFunction[Throwable, Result] = {
    case ex: DatabaseNotFoundException => NotFound(s"Database $db does not exist.")
    case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    case ex: Throwable => {
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
    }
  }

}
