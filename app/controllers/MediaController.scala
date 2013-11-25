package controllers

import play.api.mvc._
import play.api.libs.iteratee._
import play.api.libs.json._
import scala.concurrent.Future
import config.AppExecutionContexts
import play.api.{http, Logger}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/25/13
 */

case class MediaObjectIn(contentType: String, name: String, data : Array[Byte])

object MediaController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext
  import Formats._

  def save(db: String, collection: String) = repositoryAction(BodyParsers.parse.tolerantJson){
    repo => implicit req => {
        req.body.validate[MediaObjectIn] match {
          case JsError(er) => {
            Logger.warn(er.mkString("\n"))
            Future.successful(BadRequest("Invalid media object"))
          }
          case JsSuccess(m, _) => {
            repo
              .saveMedia(db, collection, Enumerator.enumerate(List(m.data)), m.name, Some(m.contentType))
              .map[Result](res => {
                  val url = routes.MediaController.get(db, collection, res.id).url
                  MediaMetadataResource(res.id, res.md5, url)})
              .recover(commonExceptionHandler(db,collection))
          }
        }
      }
    }


  def get(db: String, collection: String, id: String) = repositoryAction {
    repo => implicit req => {
      repo.getMedia(db, collection, id).map[Result]( res => MediaReaderResource(res) )
      .recover(commonExceptionHandler(db,collection))
    }
  }

  //TODO -- complete implementation
  def delete(db: String, collection: String, id: String) = Action { req => InternalServerError }


}
