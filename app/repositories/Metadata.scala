package repositories

import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */
trait Metadata {

  def envelope: Envelope

  def stats: Map[String, Int]

  def name: String

  def level: Int

}
