package controllers

import config.AppExecutionContexts
import controllers.Formats._
import play.api.Logger
import play.api.libs.json.{JsSuccess, JsError}
import play.api.mvc.{SimpleResult, BodyParsers}

import scala.concurrent.Future

object IndexController extends AbstractNoSqlController{

  import AppExecutionContexts._

  def put(db: String, collection: String, indexName: String) = repositoryAction(BodyParsers.parse.tolerantJson) {
    implicit req => {
      req.body.validate[IndexDef] match {
        case JsError(er) => {
          Logger.warn(er.mkString("\n"))
          Future.successful(BadRequest("Invalid index definition: " + er.mkString("\n")))
        }
        case JsSuccess(indexDef, _) => {
          repository.createIndex(db, collection, indexDef.copy(name = indexName))
            .map[SimpleResult] (_ =>
                Created.withHeaders("Location" -> controllers.routes.ViewController.get(db, collection, indexName).url)
          ).recover(commonExceptionHandler(db, collection))
        }
      }
    }
  }

  def list(db: String, collection: String) = repositoryAction {
    implicit req => {
      repository.getIndices(db, collection).map[SimpleResult]{ indexNames => IndexDefsResource(db, collection, indexNames)  }
    }.recover(commonExceptionHandler(db, collection))
  }

  def get(db: String, collection: String, index: String) = repositoryAction {
    implicit req => {
      repository.getIndex(db, collection, index).map[SimpleResult]{ indexDef => IndexDefResource(db, collection, indexDef)  }
    }.recover(commonExceptionHandler(db, collection))
  }

  def delete(db: String, collection: String, index: String) = repositoryAction {
    implicit req => {
      repository.dropIndex(db, collection, index).map[SimpleResult]{ _ => Ok  }
    }.recover(commonExceptionHandler(db, collection))
  }

}