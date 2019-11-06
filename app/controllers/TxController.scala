package controllers

import javax.inject.Inject

import Exceptions._
import akka.stream.scaladsl.{ JsonFraming, Keep, Sink }
import config.AppExecutionContexts
import metrics.Instrumentation
import persistence.querylang.{ BooleanExpr, QueryParser }
import persistence.Repository
import play.api.libs.json._
import play.api.libs.streams.Accumulator
import play.api.mvc._
import utilities.Utils

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
class TxController @Inject() (val repository: Repository, val instrumentation: Instrumentation, val parsers: PlayBodyParsers)
  extends InjectedController
  with RepositoryAction
  with ExceptionHandlers {

  import AppExecutionContexts.streamContext

  def insert(db: String, col: String) = {
    val writer = repository.writer(db, col)
    val parser = bodyParser(db, writer.insert, config.Constants.chunkSeparator)
    Action(parser)(_.body)
  }

  private def parse(js: JsValue): Try[BooleanExpr] = js match {
    case JsString(s) => QueryParser.parse(s)
    case _           => Failure(new RuntimeException("Query expression is not a string"))
  }

  def remove(db: String, col: String) = withRepository {
    implicit request =>
      {
        extract[JsString](request.body.asJson, "query") flatMap (parse(_)) match {
          case Success(q) => repository.delete(db, col, q)
            .map(_ => Ok("Objects removed"))
            .recover(commonExceptionHandler(db))
          case Failure(e) => Future.successful(BadRequest(s"Invalid parameters: ${e.getMessage} "))
        }
      }
  }

  def update(db: String, col: String) = withRepository {
    implicit request =>
      {
        val tq = extract[JsString](request.body.asJson, "query").flatMap(parse(_))
        val td = extract[JsObject](request.body.asJson, "update")
        (tq, td) match {
          case (Success(q), Success(d)) =>
            repository
              .update(db, col, q, d)
              .map(n => Ok(s"n objects updated."))
              .recover(commonExceptionHandler(db))
          case _ => Future.successful(BadRequest(s"Invalid Request body"))
        }
      }
  }

  def upsert(db: String, col: String) = {
    val writer = repository.writer(db, col)
    val parser = bodyParser(db, writer.upsert, config.Constants.chunkSeparator).map(f => f)
    Action(parser)(_.body)
  }

  private def extract[T <: JsValue: ClassTag](in: Option[JsValue], key: String): Try[T] =
    Try {
      in.map {
        js => (js \ key).getOrElse(JsNull)
      } match {
        case Some(v: T) => v
        case _          => throw new InvalidParamsException(s"Request body $in isn't a Json with an  $key property of correct type")
      }
    }

  /**
   * Creates a new BodyParser that deserializes GeoJson features in the input and
   * writes them to the specified database and collection.
   *
   * @param db the database to write features to
   * @return a new BodyParser
   */
  private def bodyParser(db: String, writer: Seq[JsObject] => Future[Int], sep: String): BodyParser[Result] =
    BodyParser("GeoJSON feature BodyParser") { request =>
      //TODO -- the "magic" numbers should be documented and configurable.
      //TODO -- Better to refactor FeatureWriter to a Sink, and have factory method for that Sink in the Repository
      val flow = JsonFraming.objectScanner(1024 * 1024)
        .map(_.utf8String)
        .map(s => Utils.withDebug(s"seen: $s") { s })
        .map(Json.parse)
        .collect { case js: JsObject => js } //TODO -- log where there is a parse failure
        .grouped(128)
        .mapAsync(2)(writer.apply) //TODO -- better error-handling, using
        .toMat(Sink.fold(0)(_ + _))(Keep.right)

      Accumulator(flow).map { r =>
        Right(Ok(s"Written $r features"))
      }.recover {
        commonExceptionHandler(db).andThen(Left.apply)
      }
    }

}

