package controllers

import util.MediaTypeSpec
import play.api.mvc.{RequestHeader, Result, Action, Controller}
import repositories.MongoRepository
import play.api.libs.json._
import config.ConfigurationValues.Format

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object Databases extends Controller {

  def toResultRepr[A](result: A)(implicit request: RequestHeader, writes: Writes[A]): Result = {
    render {
      case MediaTypeSpec(Format.JSON, version) => Ok(Json.toJson(result)).as(MediaTypeSpec(Format.JSON, version))
      case _ => UnsupportedMediaType("No supported media type")
    }
  }

  def list() = {
    Action {
      implicit request =>
        val dbs = MongoRepository.listDatabases()
        val results = dbs.map(name => Map("name" -> name, "url" -> routes.Databases.getDb(name).url))
        toResultRepr(results)
    }
  }

  def getDb(db: String) = Action {
    implicit request =>
      MongoRepository.listCollections(db) match {
        case None => NotFound(s"No database $db")
        case Some(collections) => {
          val result = collections map (n => Map(n -> routes.Databases.getCollection(db, n).url))
          toResultRepr(result)
        }
      }
  }

  def createDb(db: String) = Action {
    implicit request =>
      MongoRepository.createDb(db) match {
        case true => Created(s"database $db created")
        case false => Conflict(s"datase $db already exists.")
      }
  }

  def deleteDb(db: String) = Action {
    implicit request =>
      MongoRepository.deleteDb(db) match {
        case true => Ok(s"database $db dropped.")
        case false => NotFound(s"No database $db")
      }

  }

  def getCollection(db: String, collection: String) = Action {
    implicit request =>
      Ok("ok")
  }

}
