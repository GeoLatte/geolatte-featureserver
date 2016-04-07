package metrics

import io.prometheus.client._
import io.prometheus.client.hotspot.DefaultExports
import play.api.Logger

/**
  * Created by Karel Maesen, Geovise BVBA on 22/02/16.
  */
class PrometheusMetrics {

  val totalRequests = Counter.build()
    .name("requests_total").help("Total requests.").register()

  val failedRequests = Counter.build()
    .name("requests_failed_total").help("Total failed requests.").register()

    val requestLatency = Histogram.build()
    .name("requests_latency_seconds").help("Request latency in seconds.").register();

  def start() {
    Logger.info(s"Starting the PrometheusMetrics...")
    CollectorRegistry.defaultRegistry.clear()
    CollectorRegistry.defaultRegistry.register( totalRequests )
    CollectorRegistry.defaultRegistry.register( failedRequests )
    CollectorRegistry.defaultRegistry.register( requestLatency )

    // Register the default Hotspot collectors
    DefaultExports.initialize()
    Logger.info(s"PrometheusMetrics started with Default hotspot collectors")
  }

  def stop(): Unit = {
    Logger.info(s"Stopping PrometheusMetrics....")
    CollectorRegistry.defaultRegistry.clear()
    Logger.info(s"Prometheus CollectorRegistry cleared.")
  }

}
