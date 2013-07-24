package repositories

import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */

trait Metadata

case class NonSpatialMetadata(name: String) extends Metadata


case class SpatialMetadata(name: String, envelope: Envelope, stats: Map[String, Int], level : Int) extends Metadata