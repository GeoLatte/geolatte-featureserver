package controllers

import util._
import util.CustomBodyParsers._
import play.api.mvc.{RequestHeader, Result, Action, Controller}
import repositories.MongoRepository
import config.ConfigurationValues.Format
import models._
import play.api.libs.json.{JsValue, JsError, JsNull}
import scala.util.{Failure, Success}
import scala.concurrent.Future


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
        val dbs = MongoRepository.listDatabases
        toResult(DatabasesResource(dbs))
    }
  }

  def getDb(db: String) = Action {
    implicit request =>
      val fc = MongoRepository.listCollections(db)
       Async {
            fc.map ( colls => toResult(DatabaseResource(db, colls)) ).fallbackTo(
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

//  def createCollection(db: String, col: String) = Action (tolerantNullableJson) {
//    implicit request => {
//      //interpret request body
//      import models.CollectionResourceReads._
//      val spatialSpec: Either[JsValue, Option[SpatialSpec]] = request.body match {
//        case JsNull => Right(None)
//        case js: JsValue => js.validate[SpatialSpec].fold(
//          invalid = errs => Left(JsError.toFlatJson(errs)),
//          valid = v => Right(Some(v)))
//      }
//      spatialSpec match {
//        case Right(opt) => MongoRepository.createCollection(db, col, opt) match {
//          case false => Conflict(s"$db doesn't exist, or $col already exists") //This is not very consistent: 404 and 409 conflated
//          case true => Ok(s"$db/$col created")
//        }
//        case Left(errs) => BadRequest("Invalid format: " + errs)
//      }
//    }
//  }

  def deleteCollection(db: String, col: String) = Action(tolerantNullableJson) {
    implicit request =>
      Async{
        MongoRepository.deleteCollection(db, col) map {
          case false => NotFound(s"Collection $db/$col not found.")
          case true => Ok(s"Collection $db/$col deleted.")
        }
      }
  }
}
