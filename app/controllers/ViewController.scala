package controllers

import play.api.libs.json._
import play.api.mvc.{Result, BodyParsers}
import play.api.Logger
import scala.concurrent.Future
import config.AppExecutionContexts
import utilities.{QueryParam, SupportedMediaTypes}
import config.ConfigurationValues.Format

object ViewController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext
  import Formats._

  object QueryParams {
    val NAME = QueryParam("name", (s) => Some(s))
  }

  def save(db: String, collection: String) = repositoryAction(BodyParsers.parse.tolerantJson) {
    repo => implicit req => {
      req.body.validate(ViewDefIn) match {
        case JsError(er) => {
          Logger.warn(er.mkString("\n"))
          Future.successful(BadRequest("Invalid view definition: " + er.mkString("\n")))
        }
        case JsSuccess(js, _) => {
          repo
            .saveView(db, collection, js)
            .map[Result](res => Created.withHeaders("Location" -> controllers.routes.ViewController.get(db, collection, res).url))
            .recover(commonExceptionHandler(db, collection))
        }
      }
    }
  }

  def list(db: String, collection: String) = repositoryAction {
    repo => implicit req => {
      implicit val qstr = req.queryString
      QueryParams.NAME.extract.map(name =>
        repo.getViewByName(db, collection, name)
          .map(viewDef2Result(db, collection))
      ).orElse {
        Some(
          repo.getViews(db, collection)
            .map(res => res.map(el => format(db, collection)(el)))
            .map(res => Ok(JsArray(res)).as(SupportedMediaTypes(Format.JSON)))
        )
      }.get.recover(commonExceptionHandler(db, collection))
    }
  }


  def get(db: String, collection: String, id: String) = repositoryAction {
    repo => implicit req => {
      repo.getView(db, collection, id)
        .map(viewDef2Result(db, collection))
        .recover(commonExceptionHandler(db, collection))
    }
  }

  private def format(db: String, col: String)(viewDef: JsObject): JsObject = {
    val outFormat = ViewDefOut(db, col)
    viewDef.validate(outFormat) match {
      case JsSuccess(out, _) => out
      case _ => throw new RuntimeException("Error when formatting view definition.")
    }
  }

  private def viewDef2Result(db: String, col: String)(viewDef: JsObject): Result =
    Ok(format(db: String, col: String)(viewDef)).as(SupportedMediaTypes(Format.JSON))
}
