package util

import org.geolatte.geom.curve.{MortonCode, MortonContext}
import org.geolatte.common.Feature
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import reactivemongo.bson._
import org.geolatte.nosql.mongodb._
import reactivemongo.api.Cursor
import scala.Some
import reactivemongo.api.collections.default.BSONCollection
import org.geolatte.nosql.json.MongoWriter
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.json.JsObject

/**
 * A Sink that
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/4/13
 */
case class MongoDbSink(database: String, collection: String, BUFSIZE: Int = 5000) extends Sink[JsObject] {

  lazy val writer = MongoWriter(database, collection)

  def in(objs: Enumerator[JsObject]) = {
    val takeBufsize = Enumeratee.take[JsObject](BUFSIZE) &>> Iteratee.getChunks
    val group = Enumeratee.grouped(takeBufsize)
    objs |>>> (group &>> Iteratee.foreach(fseq => writer.add(fseq)))
  }

}

//TODO -- create MongoDbSource