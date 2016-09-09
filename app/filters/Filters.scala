package filters

import javax.inject.Inject

import akka.stream.Materializer
import metrics.Metrics
import org.slf4j.LoggerFactory
import play.api.http.DefaultHttpFilters
import play.api.mvc._
import play.filters.gzip.GzipFilter

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class LoggingFilter @Inject() (implicit val metrics: Metrics, val mat: Materializer, ec: ExecutionContext) extends Filter {

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val startTime = System.currentTimeMillis
    val timer = metrics.prometheusMetrics.requestLatency.startTimer()

    val requestLogger = LoggerFactory.getLogger("requests")

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

}

class Filters @Inject() (gzip: GzipFilter, logger: LoggingFilter ) extends DefaultHttpFilters(gzip, logger)