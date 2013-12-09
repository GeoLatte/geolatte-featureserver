package controllers

import play.api.mvc._
import play.api.libs.iteratee._
import play.api.libs.json._
import scala.concurrent.Future
import config.AppExecutionContexts
import play.api.{http, Logger}
import nosql.mongodb.Repository

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/25/13
 */

case class MediaObjectIn(contentType: String, name: String, data : Array[Byte])

object MediaController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext
  import Formats._

  def save(db: String, collection: String) = repositoryAction(BodyParsers.parse.tolerantJson){
    implicit req => {
        req.body.validate[MediaObjectIn] match {
          case JsError(er) => {
            Logger.warn(er.mkString("\n"))
            Future.successful(BadRequest("Invalid media object"))
          }
          case JsSuccess(m, _) => {
            Repository
              .saveMedia(db, collection, Enumerator.enumerate(List(m.data)), m.name, Some(m.contentType))
              .map[SimpleResult](res => {
                  val url = routes.MediaController.get(db, collection, res.id).url
                  MediaMetadataResource(res.id, res.md5, url)})
              .recover(commonExceptionHandler(db,collection))
          }
        }
      }
    }


  def get(db: String, collection: String, id: String) = repositoryAction {
    implicit req => {
      Repository.getMedia(db, collection, id).map[SimpleResult]( res => MediaReaderResource(res) )
      .recover(commonExceptionHandler(db,collection))
    }
  }

  //TODO -- complete implementation
  def delete(db: String, collection: String, id: String) = Action { req => InternalServerError }


}
