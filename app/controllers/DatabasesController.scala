package controllers

import play.api.mvc._
import play.api.libs.json.{Json, JsValue, JsError, JsNull}
import scala.concurrent.Future
import play.Logger
import scala.Some
import nosql.Exceptions._
import nosql.mongodb.{Metadata, MongoRepository}


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/22/13
 */
object DatabasesController extends AbstractNoSqlController {

  import config.AppExecutionContexts.streamContext

  def list() = repositoryAction(
    repo => implicit request => repo.listDatabases.map[Result](dbs => DatabasesResource(dbs)).recover {
      case ex =>
        Logger.error(s"Couldn't list databases : ${ex.getMessage}")
        InternalServerError(ex.getMessage)
    }
  )

  def getDb(db: String) = repositoryAction(
    repo => implicit request  => repo.listCollections(db).map[Result](colls => {
      Logger.info("collections found: " + colls)
      DatabaseResource(db, colls)
    }).recover(commonExceptionHandler(db))
  )


  def putDb(db: String) = repositoryAction (
    repo => implicit request => repo.createDb(db).map(_ => Created(s"database $db created") ).recover {
          case ex : DatabaseAlreadyExists => Conflict(ex.getMessage)
          case ex : DatabaseCreationException => {
            Logger.error("Error: creating database", ex)
            InternalServerError(ex.getMessage)
          }
          case t => {
            Logger.error("Error: creating database", t)
            InternalServerError(s"${t.getMessage}")
          }
        }
  )


  def deleteDb(db: String) = repositoryAction ( repo => implicit request =>
        repo.dropDb(db).map( _ => Ok(s"database $db dropped") )
          .recover (commonExceptionHandler(db))
  )

  def getCollection(db: String, collection: String) = repositoryAction ( repo => implicit request =>
    repo.metadata(db, collection).map[Result](md => CollectionResource(md))
          .recover(commonExceptionHandler(db,collection))
  )

  def createCollection(db: String, col: String) = Action(BodyParsers.parse.tolerantJson) {
    implicit request => {

      def parse(body: JsValue) = body match {
        case JsNull => Left(Json.obj("error" -> "Received empty request body (null json)."))
        case js: JsValue => js.validate(Formats.CollectionFormat).fold(
          invalid = errs => Left(JsError.toFlatJson(errs)),
          valid = v => Right(Some(v)))
        case _ => Left(Json.obj("error" -> "Received no request body."))
      }

      def doCreate(spatialSpecOpt: Option[Metadata]) = {
        repository.createCollection(db, col, spatialSpecOpt).map(_ => Created(s"$db/$col ")).recover {
          case ex: DatabaseNotFoundException => NotFound(s"No database $db")
          case ex: CollectionAlreadyExists => Conflict(s"Collection $db/$col already exists.")
          case ex: Throwable => InternalServerError(s"{ex.getMessage}")
        }
      }

      Async {
        parse(request.body) match {
          case Right(spatialSpecOpt) => doCreate(spatialSpecOpt)
          case Left(errs) => Future.successful(BadRequest("Invalid JSON format: " + errs))
        }

      }
    }
  }

  def deleteCollection(db: String, col: String) = repositoryAction (repo => implicit request =>
    repo.deleteCollection(db, col).map(_ => Ok(s"Collection $db/$col deleted."))
          .recover(commonExceptionHandler(db,col)))

}
