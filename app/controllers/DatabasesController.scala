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
import Exceptions._


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object Databases extends AbstractNoSqlController {

  import config.AppExecutionContexts.streamContext;

  //TODO -- also set the Vary response header
  // TODO move into a separate trait
  def toResult[A <: RenderableResource](result: A)(implicit request: RequestHeader): Result = {
    (result, request) match {
      case (r : Jsonable, MediaTypeSpec(Format.JSON, version)) => Ok(r.toJson).as(MediaTypeSpec(Format.JSON, version))
      case (r : Csvable, MediaTypeSpec(Format.CSV, version)) => Ok(r.toCsv).as(MediaTypeSpec(Format.CSV, version))
      case _ => UnsupportedMediaType("No supported media type: " + request.acceptedTypes.mkString(";"))
    }
  }

  def list() = {
    Action {
      implicit request =>
        Async {
          MongoRepository.listDatabases.map( dbs => toResult(DatabasesResource(dbs)) )
            .recover { case ex =>
                Logger.error(s"Couldn't list databases : ${ex.getMessage}")
                InternalServerError(ex.getMessage) }
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
            } ).recover (commonExceptionHandler(db))
          }
       }


  def createDb(db: String) = Action(tolerantNullableJson) {
    implicit request =>

      Async {
        MongoRepository.createDb(db).map(_ => Created(s"database $db created") ).recover{
          case ex : DatabaseAlreadyExists => Conflict(ex.getMessage)
          case ex : DatabaseCreationException => InternalServerError(ex.getMessage)
          case t => InternalServerError(s"${t.getMessage}")
        }
      }
  }

  def deleteDb(db: String) = Action (tolerantNullableJson) {
    implicit request =>
      Async{
        MongoRepository.deleteDb(db).map( _ => Ok(s"database $db dropped") )
          .recover (commonExceptionHandler(db))
      }
  }

  def getCollection(db: String, collection: String) = Action {
    implicit request =>
      Async{
        MongoRepository.metadata(db, collection).map( md => toResult(CollectionResource(md)) )
          .recover(commonExceptionHandler(db,collection))
      }
  }

  def createCollection(db: String, col: String) = Action(tolerantNullableJson) {
    implicit request => {

      import models.CollectionResourceReads._

      def parse(body: JsValue) = body match {
        case JsNull => Right(None)
        case js: JsValue => js.validate[SpatialSpec].fold(
          invalid = errs => Left(JsError.toFlatJson(errs)),
          valid = v => Right(Some(v)))
      }

      def doCreate(spatialSpecOpt: Option[SpatialSpec]) = {
        MongoRepository.createCollection(db, col, spatialSpecOpt).map(_ => Ok(s"$db/$col ")).recover {
          case ex: DatabaseNotFoundException => NotFound(s"No database $db")
          case ex: CollectionAlreadyExists => Conflict(s"Collection $db/$col already exists.")
          case ex: Throwable => InternalServerError(s"{ex.getMessage}")
        }
      }

      Async {
        parse(request.body) match {
          case Right(spatialSpecOpt) => doCreate(spatialSpecOpt)
          case Left(errs) => Future.successful(BadRequest("Invalid JSON format: " + errs))
        }

      }
    }
  }

  def deleteCollection(db: String, col: String) = Action(tolerantNullableJson) {
    implicit request =>
      Async {
        MongoRepository.deleteCollection(db, col).map(_ => Ok(s"Collection $db/$col deleted."))
          .recover(commonExceptionHandler(db,col))
      }
  }

}
