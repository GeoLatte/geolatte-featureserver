package util

import org.geolatte.common._
import dataformats.json.jackson.JsonMapper
import java.io.{IOException, FileNotFoundException}
import org.geolatte.geom.Envelope
import reactivemongo.api.Cursor
import play.api.libs.iteratee.{Enumerator, Iteratee}


//TODO -- references to Cursors should be replaced by Enumerator !!!!

/**
 * A Sink for data
 */
trait Sink[A] {
  def in(enumerator: Enumerator[A]): Unit
}

trait Source[A] {
  def out(): Enumerator[A]
}

/*
 Creates Sinks that write to Standard output. Mainly used for testing  purposes.
 */
object StdOutSink {

  /**
   *
    *@param f a function that turns an instance of A into a printable representation
   * @tparam A Type of objects that go into the sink
   * @return
   */
 def apply[A](f: A => String): Sink[A] =
    new Sink[A] {
      def in(enumerator: Enumerator[A]) {
        enumerator.apply(Iteratee.foreach { a => println( f(a) )})
      }

    }

}
