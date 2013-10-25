package controllers

import play.api.mvc._
import nosql.Exceptions._
import utilities.SupportedMediaTypes
import config.ConfigurationValues.Format

import play.Logger
import nosql.mongodb._

import scala.language.implicitConversions
import scala.concurrent.Future
import nosql.Exceptions.CollectionNotFoundException
import nosql.Exceptions.DatabaseNotFoundException

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller {

  implicit val repository : Repository = MongoRepository

  def repositoryAction[T](bp : BodyParser[T])(action: Repository => Request[T] => Future[Result])
                         (implicit repo : Repository) = Action(bp) {
         request =>
            Async {
              action(repo)(request)
            }
        }

  def repositoryAction(action: Repository => Request[AnyContent] => Future[Result]) (implicit repo : Repository) : Action[AnyContent] =
    repositoryAction(BodyParsers.parse.anyContent)(action)(repo)

  implicit def toResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): Result = {
    (result, request) match {
      case (r : Jsonable, SupportedMediaTypes(Format.JSON, version)) => Ok(r.toJson).as(SupportedMediaTypes(Format.JSON, version))
      case (r : Csvable, SupportedMediaTypes(Format.CSV, version)) => Ok(r.toCsv).as(SupportedMediaTypes(Format.CSV, version))
      case _ => UnsupportedMediaType("No supported media type: " + request.acceptedTypes.mkString(";"))
    }
  }

  def commonExceptionHandler(db : String, col : String = "") : PartialFunction[Throwable, Result] = {
    case ex: DatabaseNotFoundException => NotFound(s"Database $db does not exist.")
    case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    case ex: MediaObjectNotFoundException => NotFound(s"Media object does not exist.")
    case ex: Throwable => {
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
    }
  }

}
