package controllers

import javax.inject.Inject

import persistence.{ Metadata, Repository }
import play.api.mvc._
import play.api.libs.json._

import scala.concurrent.Future
import Exceptions._
import metrics.Instrumentation
import utilities.Utils

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
class DatabasesController @Inject() (val repository: Repository, val instrumentation: Instrumentation, val parsers: PlayBodyParsers)
  extends InjectedController
  with RepositoryAction
  with ExceptionHandlers {

  import config.AppExecutionContexts.streamContext

  import ResourceWriteables._

  def list() = withRepository {
    implicit request =>
      repository.listDatabases map DatabasesResource map (Ok(_)) recover commonExceptionHandler
  }

  def getDb(db: String) = withRepository {
    implicit request =>
      repository.listCollections(db) map (DatabaseResource(db, _)) map (Ok(_)) recover commonExceptionHandler(db)
  }

  def putDb(db: String) = withRepository {
    implicit request =>
      repository.createDb(db).map(_ => Created(s"database $db created")).recover {
        commonExceptionHandler(db)
      }
  }

  def deleteDb(db: String) = withRepository(implicit request =>
    repository.dropDb(db).map(_ => Ok(s"database $db dropped"))
      .recover { case DatabaseNotFoundException(_) => Ok(s"database $db doesn't exist") }
      .recover(commonExceptionHandler(db)))

  def getCollection(db: String, collection: String) = withRepository {
    implicit request =>
      repository.metadata(db, collection, withCount = true)
        .map(CollectionResource)
        .map(Ok(_))
        .recover(commonExceptionHandler(db, collection))
  }

  def createCollection(db: String, col: String) = Action.async(parsers.tolerantJson) {
    implicit request =>
      {
        import metrics.Operation
        def parse(body: JsValue) = body match {

          case JsNull => Left(Json.obj("error" -> "Received empty request body (null json)."))
          case js: JsValue => js.validate(Formats.CollectionReadsForJsonTable).fold(
            invalid = errs => Left(JsError.toJson(errs)),
            valid = v => Right(v)
          )
          case _ => Left(Json.obj("error" -> "Received no request body."))
        }

        def doCreate(metadata: Metadata) = {
          repository.createCollection(db, col, metadata).map(_ => Created(s"$db/$col "))
            .andThen { case _ => instrumentation.incrementOperation(Operation.CREATE_TABLE, db, col) }
            .recover {
              case ex: DatabaseNotFoundException        => NotFound(s"No database $db")
              case ex: CollectionAlreadyExistsException => Conflict(s"Collection $db/$col already exists.")
              case ex: Throwable                        => InternalServerError(s"${Utils.withError(s"$ex") { ex.getMessage }}")
            }
        }

        parse(request.body) match {
          case Right(spatialSpecOpt) => doCreate(spatialSpecOpt)
          case Left(errs)            => Future.successful(BadRequest("Invalid JSON format: " + errs))
        }

      }
  }

  def registerCollection(db: String) = withRepository {

    implicit request =>
      {
        import Utils._
        (for {
          js <- request.body.asJson.future(InvalidRequestException(s"Empty or Invalid JsonRequest body"))
          metadata <- js.validate(Formats.CollectionReadsForRegisteredTable).future
          _ <- repository.registerCollection(db, metadata.name, metadata)
        } yield Created(s"db/${metadata.name}")).recover {
          case t: InvalidRequestException           => BadRequest(s"${t.msg}")
          case ex: DatabaseNotFoundException        => NotFound(s"No database $db")
          case ex: CollectionAlreadyExistsException => Conflict(s"Collection already exists in $db.")
          case ex: Throwable                        => InternalServerError(s"${ex.getMessage}")
        }
      }
  }

  def deleteCollection(db: String, col: String) = withRepository(implicit request =>
    repository.deleteCollection(db, col).map(_ => Ok(s"Collection $db/$col deleted."))
      .recover { case CollectionNotFoundException(_) => Ok(s"Collection $col doesn't exist") }
      .recover(commonExceptionHandler(db, col)))

}
