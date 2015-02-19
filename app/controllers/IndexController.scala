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
    }
  }

  def get(db: String, collection: String, index: String) = play.mvc.Results.TODO

  def delete(db: String, collection: String, index: String) = play.mvc.Results.TODO
}