package utilities

import Exceptions.InvalidRequestException
import play.api.Logger
import play.api.libs.json._

import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 08/04/16.
 */
object Utils {

  import scala.concurrent.ExecutionContext.Implicits.global

  def int(v: Any): Int = v.asInstanceOf[Int]
  def string(v: Any): String = v.asInstanceOf[String]
  def double(v: Any): Double = v.asInstanceOf[Double]
  def boolean(v: Any): Boolean = v.asInstanceOf[Boolean]
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

  //TODO -- should no longer be necessary after cleanup of Migrations and DatabaseController
  trait Foldable[B] {
    //by name argument in second position is vital to have proper "serializing" behavior
    def combine(b1: B, b2: => B): B
    def unit: B
  }

  implicit object BooleanFutureFoldable extends Foldable[Future[Boolean]] {
    override def combine(b1: Future[Boolean], b2: => Future[Boolean]): Future[Boolean] = b1.flatMap(bb1 => b2.map(_ && bb1))
    override def unit: Future[Boolean] = Future.successful(true)
  }

  def sequence[M, B](items: List[M])(f: M => B)(implicit ev: Foldable[B]): B =
    items.foldLeft(ev.unit) { (res, m) => ev.combine(res, f(m)) }

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

