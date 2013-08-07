package org.geolatte.nosql.mongodb

import org.geolatte.geom.Envelope
import com.mongodb.casbah.Imports._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */

case class Metadata(name: String, count: Long, spatialMetadata: Option[SpatialMetadata] = None)

case class SpatialMetadata(envelope: Envelope, level : Int)

object SpatialMetadata {

  import MetadataIdentifiers._

  def from(dbObj: DBObject): Option[SpatialMetadata] = {
      for {
        h <- dbObj.getAs[String](ExtentField)
        env <- EnvelopeSerializer.unapply(h)
        name <- dbObj.getAs[String](CollectionField)
        level <- dbObj.getAs[Int](IndexLevelField)
      } yield SpatialMetadata(env, level)
    }

}