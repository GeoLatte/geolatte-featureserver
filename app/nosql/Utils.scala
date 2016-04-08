package nosql

import play.api.Logger

import scala.concurrent.Future

/**
  * Created by Karel Maesen, Geovise BVBA on 08/04/16.
  */
object Utils {

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

  def debug[T](t: => T) : T= {
    println(t.toString)
    t
  }

  //TODO -- replace with Cats??
  trait Foldable[B] {
    def combine(b1: B, b2: B) : B
    def unit: B
  }

  implicit object BooleanFutureFoldable extends Foldable[Future[Boolean]] {
    override def combine( b1 : Future[Boolean], b2: Future[Boolean]): Future[Boolean] = b1.flatMap( bb1 => b2.map( _ && bb1))
    override def unit: Future[Boolean] = Future.successful(true)
  }

  def sequence[M,B](items: List[M])(f: M => B)(implicit ev: Foldable[B]): B =
    items.foldLeft( ev.unit ) { (res, m) => ev.combine(res, f(m)) }


}
