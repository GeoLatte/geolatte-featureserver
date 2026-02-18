package utilities

import Exceptions.InvalidRequestException
import play.api.Logger
import play.api.libs.json._

import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 08/04/16.
 */
object Utils {

  def json(v: Any): JsValue = Json.parse(v.asInstanceOf[String])

  val Logger = play.api.Logger("application")

  def withWarning[T](msg: String)(t: => T) = {
    Logger.warn(msg)
    t
  }

  def withInfo[T](msg: String)(t: => T) = {
    Logger.info(msg)
    t
  }

  def withError[T](msg: String)(t: => T) = {
    Logger.error(msg)
    t
  }

  def withError[T](msg: String, cause: Throwable)(t: => T) = {
    Logger.error(msg, cause)
    t
  }

  def withDebug[T](msg: String)(t: => T) = {
    Logger.debug(msg)
    t
  }

  def withTrace[T](msg: String)(t: => T) = {
    Logger.trace(msg)
    t
  }

  def debug[T](t: => T): T = {
    println("DEBUG: " + t.toString)
    t
  }

  def toFuture[T](opt: Option[T], failure: Throwable): Future[T] = opt match {
    case Some(t) => Future.successful(t)
    case _       => Future.failed(failure)
  }

  def toFuture[T](result: JsResult[T]): Future[T] = result match {
    case JsSuccess(t, _) => Future.successful(t)
    case JsError(errs)   => Future.failed(new InvalidRequestException(Json.stringify(JsError.toJson(errs))))
  }

  implicit class FuturableOption[T](opt: Option[T]) {
    def future(default: Throwable): Future[T] = toFuture(opt, default)
  }

  implicit class FuturableJsResult[T](result: JsResult[T]) {
    def future: Future[T] = toFuture(result)
  }

}

