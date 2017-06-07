package metrics

import io.prometheus.client._

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

  val dbio: Histogram = Histogram.build()
    .exponentialBuckets(0.1, 2.0, 10)
    .name("dbio_op_milliseconds").help("DBIO operations in milliseconds.")
    .labelNames("operation")
    .create()

  val dbioStreamComplete: Histogram = Histogram.build()
    .exponentialBuckets(1, 2.0, 10)
    .name("dbio_stream_completed_milliseconds").help("DBIO streaming finished after milliseconds.")
    .labelNames("database", "collection")
    .create()

  val dbioStreamStart: Histogram = Histogram.build()
    .exponentialBuckets(1, 2.0, 15)
    .name("dbio_stream_start_milliseconds").help("DBIO streaming received first element after milliseconds.")
    .labelNames("database", "collection")
    .create()

  val numObjectsRetrieved: Histogram = Histogram.build()
    .buckets(100, 200, 500, 1000, 1500, 2000, 3000, 4000, 5000, 7500, 10000, 15000)
    .name("number_fetched").help("Number of objects streamed from database.")
    .labelNames("database", "collection")
    .create()

  def register(registry: CollectorRegistry): Unit = {
    registry.register(totalRequests)
    registry.register(failedRequests)
    registry.register(requestLatency)
    registry.register(repoOperations)
    registry.register(bboxHistogram)
    registry.register(dbio)
    registry.register(dbioStreamStart)
    registry.register(dbioStreamComplete)
    registry.register(numObjectsRetrieved)
  }

}

