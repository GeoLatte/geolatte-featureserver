package org.geolatte.nosql.mongodb

import org.geolatte.geom.Envelope
import com.mongodb.casbah.Imports._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */

case class Metadata(name: String, count: Long, spatialMetadata: Option[SpatialMetadata] = None)

case class SpatialMetadata(envelope: Envelope, stats: Map[String, Int], level : Int)

object SpatialMetadata {

  import MetadataIdentifiers._

  def from(dbObj: DBObject): Option[SpatialMetadata] = {
      for {
        h <- dbObj.getAs[String](ExtentField)
        env <- EnvelopeSerializer.unapply(h)
        name <- dbObj.getAs[String](CollectionField)
        indexObj  <- dbObj.getAs[DBObject](IndexStatsField)
        index = indexObj.mapValues[Int]( v => toInt(v) ).toMap.filter( _._2 >= 0 )  // toMap is required to ensure mapValues result conforms to immutable.Map
        level <- dbObj.getAs[Int](IndexLevelField)
      } yield SpatialMetadata(env, index, level)
    }

    private def toInt(v: Any): Int = {
      v match {
        case i : Int => i
        case _ => -1
      }
    }

}