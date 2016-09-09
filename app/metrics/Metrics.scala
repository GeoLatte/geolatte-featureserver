package metrics

import javax.inject.Inject

import kamon.Kamon
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

/**
  * Created by Karel Maesen, Geovise BVBA on 09/09/16.
  */
trait Metrics {
  def prometheusMetrics : PrometheusMetrics
}

class FeatureServerMetrics @Inject() (lifecycle : ApplicationLifecycle) extends Metrics {

  Kamon.start()
  override val prometheusMetrics = new PrometheusMetrics
  prometheusMetrics.start()
  
  lifecycle.addStopHook(
    () => {
      Kamon.shutdown()
      Future.successful(prometheusMetrics.stop())
    }
  )


}