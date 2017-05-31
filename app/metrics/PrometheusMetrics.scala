package metrics

import io.prometheus.client._
import io.prometheus.client.hotspot.DefaultExports
import play.api.Logger

/**
 * Created by Karel Maesen, Geovise BVBA on 22/02/16.
 */
class PrometheusMetrics {

  val totalRequests = Counter.build()
    .name("requests_total").help("Number of HTTP requests.").register()

  val repoOperations = Counter.build()
    .name("operations").help("Number of featureserver operations")
    .labelNames("operation", "database", "collection")
    .register()

  val bboxHistogram = Histogram.build()
    .buckets(100, 200, 500, 1000, 5000, 10000, 25000, 50000, 75000, 100000)
    .name("spatial_queries_bbox").help("Diagonal in meters of bbox in spatial query")
    .labelNames("database", "collection")
    .register()

  // Currently not possible to efficiently retrieve this information for both Streams, featurecollections
  //  val returnedItemsHistogram = Histogram.build()
  //    .buckets(100, 200, 500, 1000, 5000, 10000, 25000, 50000, 75000, 100000)
  //    .name("spatial_queries_returned_items").help("Number of returned items by spatial query")
  //    .labelNames("database", "collection")
  //    .register()

  val failedRequests = Counter.build()
    .name("requests_failed_total").help("Number of failed requests.").register()

  val requestLatency = Histogram.build()
    .name("requests_latency_seconds").help("Request latency in seconds.").register();

  def start() {
    Logger.info(s"Starting the PrometheusMetrics...")
    CollectorRegistry.defaultRegistry.clear()
    CollectorRegistry.defaultRegistry.register(totalRequests)
    CollectorRegistry.defaultRegistry.register(failedRequests)
    CollectorRegistry.defaultRegistry.register(requestLatency)
    CollectorRegistry.defaultRegistry.register(repoOperations)
    CollectorRegistry.defaultRegistry.register(bboxHistogram)
    //    CollectorRegistry.defaultRegistry.register(returnedItemsHistogram)

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
