package org.geolatte.scala

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/5/13
 */
object Utils {

  def time[A]( a: => A ): A = {
    val now = System.currentTimeMillis
    val result = a
    val time = System.currentTimeMillis - now
    println("Operation took: %d millis." format time)
    result
  }

}
