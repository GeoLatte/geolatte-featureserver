package util

import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import org.geolatte.nosql.json.MongoWriter
import play.api.libs.json.JsObject

/**
 * A Sink that
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

//TODO -- create MongoDbSource