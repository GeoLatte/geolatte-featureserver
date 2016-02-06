package metrics

import kamon.metric.{EntityRecorderFactory, GenericEntityRecorder}
import kamon.metric.instrument.{Time, InstrumentFactory}

/**
  * Created by Karel Maesen, Geovise BVBA on 04/02/16.
  */
class RequestMetrics(instrumentFactory: InstrumentFactory) extends GenericEntityRecorder(instrumentFactory) {
  val requestExecutionTime = histogram("request-execution-time", Time.Milliseconds)
  val requests = counter("requests")
  val errors  = counter("errors")
}

object RequestMetrics extends EntityRecorderFactory[RequestMetrics] {

  override def category: String = "request"

  override def createRecorder(instrumentFactory: InstrumentFactory): RequestMetrics =
    new RequestMetrics(instrumentFactory)

}