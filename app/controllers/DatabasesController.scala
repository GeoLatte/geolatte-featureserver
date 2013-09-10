package controllers

import util._
import util.CustomBodyParsers._
import play.api.mvc._
import repositories.MongoRepository
import config.ConfigurationValues.Format
import models._
import play.api.libs.json.{JsValue, JsError, JsNull}
import scala.concurrent.Future
import play.Logger
import models.DatabaseResource
import util.SpatialSpec
import models.CollectionResource
import models.DatabasesResource
import scala.Some
import scala.util.Success


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object Databases extends Controller {

  import config.AppExecutionContexts.streamContext;

  //TODO -- also set the Vary response header
  // TODO move into a separate trait
  def toResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): Result = {
    (result, request) match {
      case (r : Jsonable, MediaTypeSpec(Format.JSON, version)) => Ok(r.toJson).as(MediaTypeSpec(Format.JSON, version))
      case (r : Csvable, MediaTypeSpec(Format.CSV, version)) => Ok(r.toCsv).as(MediaTypeSpec(Format.CSV, version))
      case _ => UnsupportedMediaType("No supported media type")
    }
  }

  def list() = {
    Action {
      implicit request =>
        Async {
          MongoRepository.listDatabases.map( dbs => toResult(DatabasesResource(dbs)) )
        }
    }
  }

  def getDb(db: String) = Action {
    implicit request =>
      val fc = MongoRepository.listCollections(db)
       Async {
            fc.map ( colls => {
              Logger.info("collections found: " + colls)
              toResult(DatabaseResource(db, colls))
            } ).fallbackTo(
              Future {NotFound(s"No database $db")}
            )
          }
       }


  def createDb(db: String) = Action(tolerantNullableJson) {
    implicit request =>
      val f = MongoRepository.createDb(db)
      Async {
        f.map{
          case true => Created(s"database $db created")
          case false => Conflict(s"datase $db already exists.")
        }.recover{ case t =>
          Logger.error("Error creating database", t)
          InternalServerError(s"${t.getMessage}")
        }
      }
  }

  def deleteDb(db: String) = Action (tolerantNullableJson) {
    implicit request =>
      Async{
        MongoRepository.deleteDb(db) map {
          case true => Ok(s"database $db dropped.")
          case false => NotFound(s"No database $db")
        }
      }
  }

  def getCollection(db: String, collection: String) = Action {
    implicit request =>
      Async{
        MongoRepository.metadata(db, collection) map ( md => toResult(CollectionResource(md)) )
      }
  }

  def createCollection(db: String, col: String) = Action(tolerantNullableJson) {
    implicit request => {
      //interpret request body
      import models.CollectionResourceReads._

      val spatialSpec: Either[JsValue, Option[SpatialSpec]] = request.body match {
        case JsNull => Right(None)
        case js: JsValue => js.validate[SpatialSpec].fold(
          invalid = errs => Left(JsError.toFlatJson(errs)),
          valid = v => Right(Some(v)))
      }

      def doCreate() = spatialSpec match {
        case Right(opt) => {
          MongoRepository.createCollection(db, col, opt) map {
            case false => InternalServerError(s"Server error while creating ${db}/${col}")
            case true => Ok(s"$db/$col created")
          }
        }
        case Left(errs) => Future.successful(BadRequest("Invalid format: " + errs))
      }

      Async {
        (for {
          dbExists <- MongoRepository.existsDb(db)
          colExists <- MongoRepository.existsCollection(db, col)
        } yield (dbExists, colExists)) flatMap {
          case (false, _) => Future.successful(NotFound(s"Database ${db} does not exist"))
          case (true, false) => doCreate()
          case (true, true) => Future.successful(Conflict(s"Collection ${col} already exists."))
        }
      }
    }
  }

  def deleteCollection(db: String, col: String) = Action(tolerantNullableJson) {
    implicit request =>
      Async{
        val existenceCondition = for {
          dbExists <- MongoRepository.existsDb(db)
          colExists <- MongoRepository.existsCollection(db,col)
        } yield dbExists && colExists

        existenceCondition.flatMap {
          case true => MongoRepository.deleteCollection(db,col) map {
            case false => InternalServerError(s"Database delete error.")
            case true => Ok(s"Collection $db/$col deleted.")
          }
          case false => Future.successful(NotFound(s"Collection $db/$col not found."))
        }
      }
  }
}
