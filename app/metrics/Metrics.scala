package metrics

import javax.inject.{ Inject, Singleton }

import com.codahale.metrics.{ MetricRegistry => DropWizardRegistry }
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.hotspot.DefaultExports
import utilities.Utils.Logger
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 09/09/16.
 */
trait Metrics {

  def prometheusMetrics: PrometheusMetrics

  def dropWizardMetricRegistry: DropWizardRegistry

  def collectorRegistry: CollectorRegistry

}

@Singleton
class FeatureServerMetrics @Inject() (lifecycle: ApplicationLifecycle) extends Metrics {

  override val prometheusMetrics: PrometheusMetrics = new PrometheusMetrics
  override val dropWizardMetricRegistry: DropWizardRegistry = new com.codahale.metrics.MetricRegistry()
  override val collectorRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

  prometheusMetrics.register(collectorRegistry)
  Logger.info("RegisterdPrometheus metrics")

  // Register the default Hotspot collectors
  // (assumes his collectorRegistry is in fact the CollectorRegistry.defaultRegistry)
  DefaultExports.initialize()
  Logger.info(s"PrometheusMetrics started with Default hotspot collectors")

  // Register the CodaHale metrics
  new DropwizardExports(dropWizardMetricRegistry).register(CollectorRegistry.defaultRegistry)
  Logger.info("Registered DropWizard Metric Registry")

  def stop: Future[Unit] = Future.successful {
    Logger.info(s"Stopping PrometheusMetrics....")
    CollectorRegistry.defaultRegistry.clear()
    Logger.info(s"Prometheus CollectorRegistry cleared.")
  }

  lifecycle.addStopHook(() => stop)

}
