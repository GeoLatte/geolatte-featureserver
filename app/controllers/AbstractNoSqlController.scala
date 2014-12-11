package controllers

import nosql.postgresql.PostgresqlRepository
import play.api.mvc._
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
import nosql._


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller with FutureInstrumented {

  import play.api.Play.current

  //TODO the resolution belongs properly in ConfigurationValues, but then should be split (definitionsettings/defaults vs configured values)
  val repository : Repository = config.ConfigurationValues.configuredRepository match {
    case "mongodb" => MongoDBRepository
    case "postgresql" => PostgresqlRepository
    case _ => sys.error("Configured with Unsupported database")
  }

  def repositoryAction[T](bp : BodyParser[T])(action: Request[T] => Future[SimpleResult]) =
    Action.async(bp) { request => action(request) }


  def repositoryAction(action: Request[AnyContent] => Future[SimpleResult])  : Action[AnyContent] =
    repositoryAction(BodyParsers.parse.anyContent)(action)

  implicit def toSimpleResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): SimpleResult = {


    implicit def toStr(js : JsObject) : String = Json.stringify(js)

    (result, request) match {
      case (r : Jsonable, SupportedMediaTypes(Format.JSON, version)) => Ok(r.toJson).as(SupportedMediaTypes(Format.JSON, version).toString)
      case (r : Csvable, SupportedMediaTypes(Format.CSV, version)) => Ok(r.toCsv).as(SupportedMediaTypes(Format.CSV, version).toString)
      case (r : JsonStreamable, SupportedMediaTypes(Format.JSON, version)) => Ok.chunked(toStream(r.toJsonStream)).as(SupportedMediaTypes(Format.JSON, version).toString)
      case (r : CsvStreamable, SupportedMediaTypes(Format.CSV, version)) => Ok.chunked(toStream(r.toCsvStream)).as(SupportedMediaTypes(Format.CSV, version).toString)
      case _ => UnsupportedMediaType("No supported media type: " + request.acceptedTypes.mkString(";"))
    }
  }

  //Note: this can't be made implicit, because in the case of A == JsValue, Ok.stream accepts features enum, and
  // this code won't be called
  def toStream[A](features: Enumerator[A])(implicit toStr: A => String) : Enumerator[Array[Byte]] = {

    val finalSeparatorEnumerator = Enumerator.enumerate(List(ConfigurationValues.jsonSeparator.getBytes("UTF-8")))

    val startTime = System.nanoTime()

    /**
     * Adds side-effecting timer toe aan Play enumInput
     *
     * TODO -- solve by better by wrapping the futureTimed in a new Enumeratee
     *
     */
    def enumInput[E](e: Input[E]) = new Enumerator[E] {
        def apply[A](i: Iteratee[E, A]): Future[Iteratee[E, A]] =
            futureTimed("json-feature-enumerator", startTime) {i.fold {
              case Step.Cont(k) => Future(k(e))
              case _ => Future.successful(i)
            }
          }
      }

     //this is due to James Roper (see https://groups.google.com/forum/#!topic/play-framework/PrPTIrLdPmY)
     class CommaSeparate extends Enumeratee.CheckDone[String, String] {
       val start = System.currentTimeMillis()
       def continue[A](k: K[String, A]) = Cont {
         case in @ (Input.Empty) => this &> k(in)
         case in: Input.El[String] => Enumeratee.map[String](ConfigurationValues.jsonSeparator + _) &> k(in)
         case Input.EOF => Done(Cont(k), Input.EOF)
       }
     }
     val commaSeparate = new CommaSeparate
     val jsons = features.map( f => toStr(f)) &> commaSeparate
     val toBytes = Enumeratee.map[String]( _.getBytes("UTF-8") )
     (jsons &> toBytes) andThen finalSeparatorEnumerator andThen enumInput(Input.EOF) //Enumerator.eof
   }

  def commonExceptionHandler(db : String, col : String = "") : PartialFunction[Throwable, SimpleResult] = {
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
