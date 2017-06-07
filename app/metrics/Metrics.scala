package metrics

import javax.inject.Inject

import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 09/09/16.
 */
trait Metrics {
  def prometheusMetrics: PrometheusMetrics
}

class FeatureServerMetrics @Inject() (lifecycle: ApplicationLifecycle) extends Metrics {

  override val prometheusMetrics = new PrometheusMetrics
  prometheusMetrics.start()

  lifecycle.addStopHook(
    () => {
      Future.successful(prometheusMetrics.stop())
    }
  )

}