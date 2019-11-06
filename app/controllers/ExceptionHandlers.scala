package controllers

import javax.inject.Inject
import Exceptions._
import metrics.Instrumentation
import persistence._
import play.Logger
import play.api.mvc._

import scala.concurrent.Future
import scala.language.{ implicitConversions, reflectiveCalls }

trait RepositoryAction {

  this: BaseController =>
  def parsers: PlayBodyParsers

  def withRepository[T](bp: BodyParser[T])(action: Request[T] => Future[Result]) =
    Action.async(bp) {
      request => action(request)
    }

  def withRepository(action: Request[AnyContent] => Future[Result]): Action[AnyContent] =
    withRepository(parsers.anyContent)(action)

}

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
    case ex: DatabaseAlreadyExistsException   => Conflict(ex.getMessage)
    case ex: UnsupportedMediaException        => UnsupportedMediaType(s"Media object does not exist.")
    case ex: IndexNotFoundException           => NotFound(ex.getMessage)
    case ex: CollectionAlreadyExistsException => Conflict(ex.getMessage)
    case ex: ViewObjectNotFoundException      => NotFound(s"View object does not exist.")
    case ex: InvalidParamsException           => BadRequest(s"Invalid parameters: ${ex.getMessage}")
    case ex: InvalidQueryException            => BadRequest(s"${ex.getMessage}")
    case ex: Throwable =>
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
  }

}
