package controllers

import play.api.mvc._
import nosql.Exceptions._
import utilities.SupportedMediaTypes
import config.ConfigurationValues.Format

import play.Logger
import nosql.mongodb._

import scala.language.implicitConversions
import scala.concurrent.Future

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller {

  implicit val repository : Repository = MongoRepository

  type RepositoryAction = Repository => Request[AnyContent] => Future[Result]

  def repositoryAction(action: => RepositoryAction)(implicit repo : Repository) = Action {
        request =>
          Async {
            action(repo)(request)
          }
      }

  def repositoryAction(bp : BodyParser[AnyContent])(action: => RepositoryAction)(implicit repo : Repository) = Action(bp) {
         request =>
            Async {
              action(repo)(request)
            }
        }

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
    case ex: Throwable => {
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
    }
  }

}
