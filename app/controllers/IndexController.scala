package controllers

import javax.inject.Inject
import config.AppExecutionContexts
import controllers.auth.{ AuthController, Rights }
import controllers.Formats._
import metrics.Instrumentation
import persistence.Repository
import utilities.Utils.Logger
import play.api.libs.json.{ JsError, JsSuccess }
import play.api.mvc.{ InjectedController, PlayBodyParsers, Result }

import scala.concurrent.Future

class IndexController @Inject() (val repository: Repository, val instrumentation: Instrumentation, val parsers: PlayBodyParsers)
  extends InjectedController
  with ExceptionHandlers
  with AuthController {

  import AppExecutionContexts._
  import ResourceWriteables._

  def put(db: String, collection: String, indexName: String) = ifHasRights(Rights.Write).async(parsers.tolerantJson) {
    implicit req =>
      {
        req.body.validate[IndexDef] match {
          case JsError(er) => {
            Logger.warn(er.mkString("\n"))
            Future.successful(BadRequest("Invalid index definition: " + er.mkString("\n")))
          }
          case JsSuccess(indexDef, _) => {
            repository.createIndex(db, collection, indexDef.copy(name = indexName))
              .map[Result](_ =>
                Created.withHeaders("Location" -> controllers.routes.ViewController.get(db, collection, indexName).url)).recover(commonExceptionHandler(db, collection))
          }
        }
      }
  }

  def list(db: String, collection: String) = ifHasRights(Rights.Read).async {
    implicit req =>
      {
        repository.getIndices(db, collection)
          .map { IndexDefsResource(db, collection, _) }
          .map { Ok(_) }
      }.recover(commonExceptionHandler(db, collection))
  }

  def get(db: String, collection: String, index: String) = ifHasRights(Rights.Read).async {
    implicit req =>
      repository.getIndex(db, collection, index)
        .map { IndexDefResource(db, collection, _) }
        .map { Ok(_) }
        .recover(commonExceptionHandler(db, collection))

  }

  def delete(db: String, collection: String, index: String) = ifHasRights(Rights.Write).async {
    implicit req =>
      repository.dropIndex(db, collection, index)
        .map[Result] { _ => Ok }
        .recover(commonExceptionHandler(db, collection))
  }

}