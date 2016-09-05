package controllers

import javax.inject.Inject

import config.AppExecutionContexts
import nosql.Repository
import nosql.mongodb.MongoDBRepository
import play.api.Logger
import play.api.libs.iteratee._
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.Future

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/25/13
 */

case class MediaObjectIn(contentType: String, name: String, data: Array[Byte])

class MediaController @Inject() (val repository: Repository) extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext
  import Formats._

  def save(db: String, collection: String) = repositoryAction(BodyParsers.parse.tolerantJson) {
    implicit req => {
      if (! repository.isInstanceOf[MongoDBRepository]) {
        Future.successful(NotImplemented)
      } else {
        req.body.validate[MediaObjectIn] match {
          case JsError(er) =>
            Logger.warn(er.mkString("\n"))
            Future.successful(BadRequest("Invalid media object"))

          case JsSuccess(m, _) =>
            repository.asInstanceOf[MongoDBRepository]
              .saveMedia(db, collection, Enumerator.enumerate(List(m.data)), m.name, Some(m.contentType))
              .map[Result](res => {
                val url = routes.MediaController.get(db, collection, res.id).url
                MediaMetadataResource(res.id, res.md5, url)
              })
              .recover(commonExceptionHandler(db, collection))
        }
      }
    }
  }


  def get(db: String, collection: String, id: String) = repositoryAction {
    implicit req =>
      if (! repository.isInstanceOf[MongoDBRepository]) {
        Future.successful(NotImplemented)
      } else {
        repository.asInstanceOf[MongoDBRepository].getMedia(db, collection, id).map[Result](res => MediaReaderResource(res))
          .recover(commonExceptionHandler(db, collection))
      }
  }

  //TODO -- complete implementation
  def delete(db: String, collection: String, id: String) = Action { req => InternalServerError}


}
