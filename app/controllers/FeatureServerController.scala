package controllers

import Exceptions._
import persistence._
import play.Logger
import play.api.mvc._

import scala.concurrent.Future
import scala.language.{ implicitConversions, reflectiveCalls }

case class RepositoryAction[T](req: Request[T])

object RepositoryAction {
  def apply[T](bp: BodyParser[T])(action: Request[T] => Future[Result]) =
    Action.async(bp) {
      request => action(request)
    }

  def apply(action: Request[AnyContent] => Future[Result]): Action[AnyContent] =
    RepositoryAction(BodyParsers.parse.anyContent)(action)

}

trait FeatureServerController extends Controller with FutureInstrumented {

  def repository: Repository

  def commonExceptionHandler(db: String, col: String = ""): PartialFunction[Throwable, Result] = {

    val baseCases: PartialFunction[Throwable, Result] = {
      case ex: DatabaseNotFoundException => NotFound(s"Database $db does not exist.")
      case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    }

    baseCases.orElse(commonExceptionHandler)
  }

  val commonExceptionHandler: PartialFunction[Throwable, Result] = {
    case ex: DatabaseAlreadyExistsException => Conflict(ex.getMessage)
    case ex: UnsupportedMediaException => UnsupportedMediaType(s"Media object does not exist.")
    case ex: IndexNotFoundException => NotFound(ex.getMessage)
    case ex: CollectionAlreadyExistsException => Conflict(ex.getMessage)
    case ex: ViewObjectNotFoundException => NotFound(s"View object does not exist.")
    case ex: InvalidParamsException => BadRequest(s"Invalid parameters: ${ex.getMessage}")
    case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
    case ex: Throwable =>
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
  }

}
