package util

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
case class SpatialSpec(crs: Int, extent: Vector[Double], level: Int) {
  def envelope: String = s"$crs:${extent(0)},${extent(1)},${extent(2)},${extent(3)}"
}