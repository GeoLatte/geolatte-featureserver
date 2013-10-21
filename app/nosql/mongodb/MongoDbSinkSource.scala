package nosql.mongodb

import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import play.api.libs.json.JsObject

import scala.language.reflectiveCalls

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
 Creates Sinks that writes to Standard output. Mainly used for testing  purposes.
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


/**
 * A Sink for GeoJson features.
 *
 * @param database the name of a database
 * @param collection the name of the collection which must already exist in the specified database
 *
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/4/13
 */
case class MongoDbSink(database: String, collection: String, BUFSIZE: Int = 5000) extends Sink[JsObject] {

  lazy val writer = MongoWriter(database, collection)

  def in(objs: Enumerator[JsObject]) = {
    import scala.concurrent.ExecutionContext.Implicits._
    val takeBufsize = Enumeratee.take[JsObject](BUFSIZE) &>> Iteratee.getChunks
    val group = Enumeratee.grouped(takeBufsize)
    objs |>>> (group &>> Iteratee.foreach(fseq => writer.add(fseq)))
  }

}

