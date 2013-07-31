package org.geolatte.nosql.json

import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.geolatte.geom.Envelope
import org.geolatte.scala.Utils._
import com.mongodb.casbah.Imports._
import org.geolatte.nosql._

import mongodb._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/3/13
 */
object Loader {

  def load(file: String, collection: String, db: String = "test", ctxt: MortonContext = new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, CrsId.valueOf(4326)), 8) ) {
    val src = GeoJsonFileSource.fromFile(file)
    lazy val coll = MongoClient()(db)(collection)
    val sink = new MongoDbSink(coll, ctxt)

    val mongoSrc = MongoDbSource(coll, ctxt)

    time(sink.in(src.out))
  }

}

