package controllers

import util.MediaTypeSpec
import play.api.mvc.{RequestHeader, Result, Action, Controller}
import repositories.MongoRepository
import config.ConfigurationValues.Format
import models._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object Databases extends Controller {

  //TODO -- also set the Vary response header.
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
        val dbs = MongoRepository.listDatabases()
        toResult(DatabasesResource(dbs))
    }
  }

  def getDb(db: String) = Action {
    implicit request =>
      MongoRepository.listCollections(db) match {
        case None => NotFound(s"No database $db")
        case Some(collections) => {
          toResult(DatabaseResource(db, collections))
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
      MongoRepository.metadata(db, collection) match {
        case Some(md) => toResult(CollectionResource(md))
        case none => NotFound(s"No metadata for collection $collection found in $db")
      }
  }

}
