package nosql

import nl.grons.metrics.scala.InstrumentedBuilder

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/7/14
 */
trait Instrumented extends InstrumentedBuilder {

  val metricRegistry = Global.metricRegistry

}
