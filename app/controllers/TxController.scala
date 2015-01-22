package controllers

import play.api.mvc._
import config.AppExecutionContexts
import querylang.{QueryParser, BooleanExpr}
import scala.concurrent.Future
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import nosql.mongodb.ReactiveGeoJson._
import nosql.InvalidParamsException
import nosql.mongodb.ReactiveGeoJson

//TODO -- this should use repository, rather than directly perform updates

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext

  def insert(db: String, col: String) = {
    val parser = mkJsonWritingBodyParser(db, col)
    Action(parser) {
      request => Async {
        request.body.map( state =>  Ok(state.warnings.mkString("\n")))
      }
    }
  }

  private def parse( js : JsValue) : Try[BooleanExpr] = js match {
    case JsString(s)  => QueryParser.parse(s)
    case _ => Failure(new RuntimeException("Query expression is not a string"))
  }

  def remove(db: String, col: String) = repositoryAction{
    implicit request  => {
      extract[JsString](request.body.asJson, "query") flatMap ( parse(_) ) match {
        case Success(q) => repository.delete(db, col, q)
          .map( _ => Ok("Objects removed"))
          .recover(commonExceptionHandler(db))
        case Failure(e) =>  Future.successful(BadRequest(s"Invalid parameters: ${e.getMessage()} "))
      }
    }
  }


  def update(db: String, col: String) = repositoryAction {
    implicit request => {
      val tq = extract[JsString](request.body.asJson, "query").flatMap( parse(_) )
      val td = extract[JsObject](request.body.asJson, "update")
      (tq,td) match {
        case (Success(q),Success(d)) =>
          repository
            .update(db,col, q, d)
            .map( n => Ok(s"n objects updated."))
            .recover(commonExceptionHandler(db))
        case _ => Future.successful(BadRequest(s"Invalid Request body"))
      }
    }
  }


  def upsert(db: String, col: String) = repositoryAction {
    implicit request => request.body.asJson match {
        case Some(obj : JsObject) => repository.upsert(db, col, obj)
          .map( _ => Ok("Objects upserted"))
          .recover(commonExceptionHandler(db))
        case _ =>  Future.successful(BadRequest(s"No Json object in request body."))
      }
  }

  private def extract[T <: JsValue : ClassTag ](in : Option[JsValue], key: String) : Try[T] =
    Try {
      in.map {
        js => js \ key
      } match {
        case Some(v: T) => v
        case _ => throw new InvalidParamsException(s"Request body isn't a Json with an  $key property of correct type")
      }
    }


  /**
   * Creates a new BodyParser that deserializes GeoJson features in the input and
   * writes them to the specified database and collection.
   *
   * @param db the database to write features to
   * @param col the collection to write features to
   * @return a new BodyParser
   */
  private def mkJsonWritingBodyParser(db: String, col: String): BodyParser[Future[State]] = {
    val writer = repository.writer(db, col)
    ReactiveGeoJson.bodyParser(writer)
  }


}

