package controllers

import play.api.mvc._
import nosql.Exceptions._
import utilities.SupportedMediaTypes
import config.ConfigurationValues.Format

import play.Logger
import nosql.mongodb._

import scala.concurrent.Future
import play.api.libs.iteratee._
import play.api.libs.json._
import config.ConfigurationValues

import scala.language.implicitConversions
import scala.language.reflectiveCalls

import config.AppExecutionContexts.streamContext


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller {

  def repositoryAction[T](bp : BodyParser[T])(action: Request[T] => Future[Result]) = Action(bp) {
         request =>
            Async {
              action(request)
            }
        }

  def repositoryAction(action: Request[AnyContent] => Future[Result])  : Action[AnyContent] =
    repositoryAction(BodyParsers.parse.anyContent)(action)

  implicit def toResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): Result = {


    implicit def toStr(js : JsObject) : String = Json.stringify(js)

    (result, request) match {
      case (r : Jsonable, SupportedMediaTypes(Format.JSON, version)) => Ok(r.toJson).as(SupportedMediaTypes(Format.JSON, version))
      case (r : Csvable, SupportedMediaTypes(Format.CSV, version)) => Ok(r.toCsv).as(SupportedMediaTypes(Format.CSV, version))
      case (r : JsonStreamable, SupportedMediaTypes(Format.JSON, version)) => Ok.stream(toStream(r.toJsonStream)).as(SupportedMediaTypes(Format.JSON, version))
      case (r : CsvStreamable, SupportedMediaTypes(Format.CSV, version)) => Ok.stream(toStream(r.toCsvStream)).as(SupportedMediaTypes(Format.CSV, version))
      case _ => UnsupportedMediaType("No supported media type: " + request.acceptedTypes.mkString(";"))
    }
  }

  //Note: this can't be made implicit, because in the case of A == JsValue, Ok.stream accepts features enum, and
  // this code won't be called
  def toStream[A](features: Enumerator[A])(implicit toStr: A => String) : Enumerator[Array[Byte]] = {

      val finalSeparatorEnumerator = Enumerator.enumerate(List(ConfigurationValues.jsonSeparator.getBytes("UTF-8")))

     //this is due to James Roper (see https://groups.google.com/forum/#!topic/play-framework/PrPTIrLdPmY)
     class CommaSeparate extends Enumeratee.CheckDone[String, String] {
       def continue[A](k: K[String, A]) = Cont {
         case in @ (Input.Empty) => this &> k(in)
         case in: Input.El[String] => Enumeratee.map[String](ConfigurationValues.jsonSeparator + _) &> k(in)
         case Input.EOF => Done(Cont(k), Input.EOF)
       }
     }

     val commaSeparate = new CommaSeparate
     val jsons = features.map( f => toStr(f)) &> commaSeparate
     val toBytes = Enumeratee.map[String]( _.getBytes("UTF-8") )
     (jsons &> toBytes) andThen finalSeparatorEnumerator andThen Enumerator.eof
   }

  def commonExceptionHandler(db : String, col : String = "") : PartialFunction[Throwable, Result] = {
    case ex: DatabaseNotFoundException => NotFound(s"Database $db does not exist.")
    case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    case ex: MediaObjectNotFoundException => NotFound(s"Media object does not exist.")
    case ex: ViewObjectNotFoundException => NotFound(s"View object does not exist.")
    case ex: InvalidParamsException => BadRequest(s"Invalid parameters: ${ex.getMessage}")
    case ex: Throwable => {
      Logger.error(s"Internal server error with message : ${ex.getMessage}", ex)
      InternalServerError(s"Internal server error ${ex.getClass.getCanonicalName} with message : ${ex.getMessage}")
    }
  }

}
