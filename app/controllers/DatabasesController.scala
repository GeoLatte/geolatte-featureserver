package controllers

import nosql.Metadata
import play.api.mvc._
import play.api.libs.json._
import scala.concurrent.Future
import play.Logger
import Exceptions._



/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object DatabasesController extends AbstractNoSqlController {

  import config.AppExecutionContexts.streamContext

  def list() = repositoryAction(
    implicit request => repository.listDatabases.map[Result](dbs => DatabasesResource(dbs)).recover {
      case ex =>
        Logger.error(s"Couldn't list databases : ${ex.getMessage}")
        InternalServerError(ex.getMessage)
    }
  )

  def getDb(db: String) = repositoryAction(
    implicit request => repository.listCollections(db).map[Result](colls => {
      Logger.info("collections found: " + colls)
      DatabaseResource(db, colls)
    }).recover(commonExceptionHandler(db))
  )


  def putDb(db: String) = repositoryAction(
    implicit request => repository.createDb(db).map(_ => Created(s"database $db created")).recover {
      case ex: DatabaseAlreadyExistsException => Conflict(ex.getMessage)
      case ex: DatabaseCreationException =>
        Logger.error("Error: creating database", ex)
        InternalServerError(ex.getMessage)
      case t =>
        Logger.error("Error: creating database", t)
        InternalServerError(s"${t.getMessage}")
    }
  )


  def deleteDb(db: String) = repositoryAction(implicit request =>
    repository.dropDb(db).map(_ => Ok(s"database $db dropped"))
      .recover { case DatabaseNotFoundException(_) => Ok(s"database $db doesn't exist") }
      .recover(commonExceptionHandler(db))
  )

  def getCollection(db: String, collection: String) = repositoryAction(implicit request =>
    repository.metadata(db, collection).map[Result](md => CollectionResource(md))
      .recover(commonExceptionHandler(db, collection))
  )

  def createCollection(db: String, col: String) = Action.async(BodyParsers.parse.tolerantJson) {
    implicit request => {

      def parse(body: JsValue) = body match {
        case JsNull => Left(Json.obj("error" -> "Received empty request body (null json)."))
        case js: JsValue => js.validate(Formats.CollectionFormat).fold(
          invalid = (errors ) => Left(JsError.toFlatJson(errors)), //is deprecated, but can't use alternative because it's private
          valid = v => Right(v))
        case _ => Left(Json.obj("error" -> "Received no request body."))
      }

      def doCreate(metadata: Metadata) = {
        repository.createCollection(db, col, metadata).map(_ => Created(s"$db/$col ")).recover {
          case ex: DatabaseNotFoundException => NotFound(s"No database $db")
          case ex: CollectionAlreadyExistsException => Conflict(s"Collection $db/$col already exists.")
          case ex: Throwable => InternalServerError(s"{ex.getMessage}")
        }
      }


      parse(request.body) match {
        case Right(spatialSpecOpt) => doCreate(spatialSpecOpt)
        case Left(errs) => Future.successful(BadRequest("Invalid JSON format: " + errs))
      }


    }
  }

  def deleteCollection(db: String, col: String) = repositoryAction(implicit request =>
    repository.deleteCollection(db, col).map(_ => Ok(s"Collection $db/$col deleted."))
      .recover { case CollectionNotFoundException(_) => Ok(s"Collection $db doesn't exist") }
      .recover(commonExceptionHandler(db, col)))

}
