package org.geolatte.nosql.json

import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.geolatte.geom.Envelope
import org.geolatte.scala.Utils._
import org.geolatte.nosql._

import mongodb._
import reactivemongo.api.MongoDriver
import scala.concurrent.ExecutionContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/3/13
 */
object Loader {

  def load(file: String, collectionName: String, db: String = "test", ctxt: MortonContext = new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, CrsId.valueOf(4326)), 8) ) {

    import reactivemongo.api.collections.default.BSONCollectionProducer
    import ExecutionContext.Implicits.global

    val src = GeoJsonFileSource.fromFile(file)
    val driver = new MongoDriver
    val connection = driver.connection(List("localhost"))
    lazy val coll = connection(db).collection(collectionName)
    val sink = new MongoDbSink(coll, ctxt)
    time(sink.in(src.out))
  }

}

