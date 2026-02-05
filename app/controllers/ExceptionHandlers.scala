package controllers

import Exceptions._
import utilities.Utils.Logger
import play.api.mvc._

import scala.language.{ implicitConversions, reflectiveCalls }

trait ExceptionHandlers {

  import Results._

  def commonExceptionHandler(db: String, col: String = ""): PartialFunction[Throwable, Result] = {

    val baseCases: PartialFunction[Throwable, Result] = {
      case ex: DatabaseNotFoundException   => NotFound(s"Database $db does not exist.")
      case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    }

    baseCases.orElse(commonExceptionHandler)
  }

  val commonExceptionHandler: PartialFunction[Throwable, Result] = {
    case ex: UnauthorizedException            => Unauthorized(ex.getMessage)
    case ex: ForbiddenException               => Forbidden(ex.getMessage)
    case ex: DatabaseAlreadyExistsException   => Conflict(ex.getMessage)
    case ex: UnsupportedMediaException        => UnsupportedMediaType(s"Media object does not exist.")
    case ex: IndexNotFoundException           => NotFound(ex.getMessage)
    case ex: CollectionAlreadyExistsException => Conflict(ex.getMessage)
    case ex: ViewObjectNotFoundException      => NotFound(s"View object does not exist.")
    case ex: InvalidParamsException           => BadRequest(s"Invalid parameters: ${ex.getMessage}")
    case ex: InvalidQueryException            => BadRequest(s"${ex.getMessage}")
    case ex: QueryTimeoutException            => RequestTimeout("Operation timed out.")
    case ex: Throwable =>
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
  }

}
