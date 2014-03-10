package nosql

import nl.grons.metrics.scala.InstrumentedBuilder
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.TimeUnit
import play.api.Logger

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/7/14
 */
trait Instrumented extends InstrumentedBuilder {

  val metricRegistry = Global.metricRegistry

}

trait FutureInstrumented extends Instrumented {

  /**
   * Times how long it taks for the future to be completed (measured from time that the action is scheduled for execution.)
   * @param name timer name
   * @param f Future-valued action
   * @param ec Execution context
   * @tparam A Type of acton result
   * @return the future produced by the action
   */
  def futureTimed[A](name: String)(f: => Future[A])(implicit ec: ExecutionContext): Future[A] = {

    val timer = metricRegistry.timer(name)
    val start = System.nanoTime()
    val res = f //force evaluation of f (do not reference f more than once, because it will trigger a full evaluation of f!!)
    res.onComplete {
      case _ => {
        timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS)
      }
    }
    res
  }




}
