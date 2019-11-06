package controllers

import javax.inject.Inject
import config.AppExecutionContexts
import controllers.Formats._
import metrics.Instrumentation
import persistence.Repository
import utilities.Utils.Logger
import play.api.libs.json.{ JsError, JsSuccess }
import play.api.mvc.{ InjectedController, PlayBodyParsers, Result }

import scala.concurrent.Future

class IndexController @Inject() (val repository: Repository, val instrumentation: Instrumentation, val parsers: PlayBodyParsers)
  extends InjectedController
  with RepositoryAction
  with ExceptionHandlers {

  import AppExecutionContexts._
  import ResourceWriteables._

  def put(db: String, collection: String, indexName: String) = withRepository(parsers.tolerantJson) {
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

  def list(db: String, collection: String) = withRepository {
    implicit req =>
      {
        repository.getIndices(db, collection)
          .map { IndexDefsResource(db, collection, _) }
          .map { Ok(_) }
      }.recover(commonExceptionHandler(db, collection))
  }

  def get(db: String, collection: String, index: String) = withRepository {
    implicit req =>
      repository.getIndex(db, collection, index)
        .map { IndexDefResource(db, collection, _) }
        .map { Ok(_) }
        .recover(commonExceptionHandler(db, collection))

  }

  def delete(db: String, collection: String, index: String) = withRepository {
    implicit req =>
      repository.dropIndex(db, collection, index)
        .map[Result] { _ => Ok }
        .recover(commonExceptionHandler(db, collection))
  }

}