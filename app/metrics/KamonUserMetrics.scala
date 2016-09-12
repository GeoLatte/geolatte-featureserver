package metrics

import kamon.metric.{ EntityRecorderFactory, GenericEntityRecorder }
import kamon.metric.instrument.{ Time, InstrumentFactory }

/**
 * Created by Karel Maesen, Geovise BVBA on 04/02/16.
 */
class KamonUserMetrics(instrumentFactory: InstrumentFactory) extends GenericEntityRecorder(instrumentFactory) {
  val requestExecutionTime = histogram("execution-time", Time.Milliseconds)
  val requests = counter("number-of-requests")
  val errors = counter("errors")
}

object KamonUserMetrics extends EntityRecorderFactory[KamonUserMetrics] {

  override def category: String = "kamon-request"

  override def createRecorder(instrumentFactory: InstrumentFactory): KamonUserMetrics =
    new KamonUserMetrics(instrumentFactory)

}