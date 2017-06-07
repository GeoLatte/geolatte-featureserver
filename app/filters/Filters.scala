package filters

import javax.inject.Inject

import akka.stream.Materializer
import metrics.Metrics
import org.slf4j.LoggerFactory
import play.api.http.{ DefaultHttpFilters, HttpFilters }
import play.api.mvc._
import play.filters.gzip.GzipFilter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }

class MetricsFilter @Inject() (implicit val metrics: Metrics, val mat: Materializer, ec: ExecutionContext) extends Filter {

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

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
        result.withHeaders("Query-Time" -> requestTime.toString)
      }
    }
  }

}

class Filters @Inject() (gzip: GzipFilter, metrics: MetricsFilter, access: AccessLogFilter)
  extends DefaultHttpFilters(access, gzip, metrics)
