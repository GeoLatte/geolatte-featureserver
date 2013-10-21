package utilities

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/4/13
 */
object Timer {

  def time[A]( a: => A ): A = {
    val now = System.currentTimeMillis
    val result = a
    val time = System.currentTimeMillis - now
    println("Operation took: %d millis." format time)
    result
  }


}
