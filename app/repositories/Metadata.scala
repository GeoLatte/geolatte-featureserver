package repositories

import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */

case class Metadata(name: String, count: Long, spatialMetadata: Option[SpatialMetadata] = None)


case class SpatialMetadata(envelope: Envelope, stats: Map[String, Int], level : Int)