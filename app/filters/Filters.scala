package filters

import javax.inject.Inject

import kamon.Kamon
import metrics.Metrics
import org.slf4j.LoggerFactory
import play.api.http.HttpFilters
import play.api.mvc.Filter
import play.filters.gzip.GzipFilter

import scala.concurrent.ExecutionContext.Implicits.global

class Filters @Inject() (gzipFilter: GzipFilter, metrics: Metrics ) extends HttpFilters {

  val requestLogger = LoggerFactory.getLogger("requests")
  val loggingFilter  = Filter { (nextFilter, requestHeader) =>
    val startTime = System.currentTimeMillis
    val timer = metrics.prometheusMetrics.requestLatency.startTimer()

    nextFilter(requestHeader).map { result =>
      if (requestHeader.path.contains("metrics"))
        result
      else {
        val endTime = System.currentTimeMillis
        val requestTime = (endTime - startTime)
        metrics.prometheusMetrics.totalRequests.inc()
        timer.observeDuration()

        if (result.header.status != 200) {
          metrics.prometheusMetrics.failedRequests.inc()
        }

        requestLogger.info(s"${requestHeader.method} ${requestHeader.uri} ; $requestTime ; ${result.header.status}")
        result.withHeaders("Request-Time" -> requestTime.toString)
      }
    }
  }

  def filters = Seq(gzipFilter, loggingFilter)
}