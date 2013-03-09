package org.geolatte.nosql.loader

import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import org.geolatte.geom.Envelope
import org.geolatte.scala.Utils._
import com.mongodb.casbah.Imports._
import org.geolatte.nosql._
import json.GeoJsonFileSource
import mongodb._


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/3/13
 */
object Loader {

  def load() {
    val mortoncode = new MortonCode(new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, CrsId.valueOf(4326)), 8))
    val src = GeoJsonFileSource.fromFile("/tmp/tiger-1.json")
    lazy val coll = MongoClient()("test")("tiger")
    val sink = new MongoDbSink(coll, mortoncode)
  //  val mongoSrc = MongoDBSource(coll, mortoncode)

    time(sink.in(src.out))
  }

}

