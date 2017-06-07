package metrics

import io.prometheus.client._
import play.api.Logger

/**
 * Created by Karel Maesen, Geovise BVBA on 22/02/16.
 */
class PrometheusMetrics {

  val totalRequests: Counter = Counter.build()
    .name("requests_total").help("Number of HTTP requests.").create()

  val repoOperations: Counter = Counter.build()
    .name("operations").help("Number of featureserver operations")
    .labelNames("operation", "database", "collection")
    .create()

  val bboxHistogram: Histogram = Histogram.build()
    .buckets(100, 200, 500, 1000, 5000, 10000, 25000, 50000, 75000, 100000)
    .name("spatial_queries_bbox").help("Diagonal in meters of bbox in spatial query")
    .labelNames("database", "collection")
    .create()

  val failedRequests: Counter = Counter.build()
    .name("requests_failed_total").help("Number of failed requests.").create()

  val requestLatency: Histogram = Histogram.build()
    .name("requests_latency_seconds").help("Request latency in seconds.").create()

  def register(registry: CollectorRegistry): Unit = {
    registry.register(totalRequests)
    registry.register(failedRequests)
    registry.register(requestLatency)
    registry.register(repoOperations)
    registry.register(bboxHistogram)
  }

}

