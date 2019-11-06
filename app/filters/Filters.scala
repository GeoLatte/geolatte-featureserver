package filters

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import javax.inject.Inject

import akka.stream.Materializer
import metrics.Metrics
import net.logstash.logback.marker.LogstashMarker
import net.logstash.logback.marker.Markers.appendEntries
import org.slf4j.{ Logger, LoggerFactory }
import play.api.http.DefaultHttpFilters
import play.api.mvc._
import play.filters.gzip.GzipFilter

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

class MetricsFilter @Inject() (implicit val metrics: Metrics, val mat: Materializer, ec: ExecutionContext) extends Filter {

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val startTime = System.currentTimeMillis
    val timer = metrics.prometheusMetrics.requestLatency.startTimer()

    nextFilter(requestHeader).map { result =>
      if (requestHeader.path.contains("metrics"))
        result
      else {
        val endTime = System.currentTimeMillis
        val requestTime = endTime - startTime
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

/**
 * Play filter for logging all HTTP requests and responses
 *
 */
class AccessLogFilter @Inject() (implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val logger: Logger = LoggerFactory.getLogger("requests")

  /**
   * Hold log info
   */
  case class RequestLogInfo(
    startTime:       LocalDateTime,
    uri:             String,
    method:          String,
    secure:          Boolean,
    headers:         Map[String, Seq[String]],
    queryParameters: Map[String, Seq[String]],
    rawQueryString:  String
  ) {

    override def toString: String = s"$method $uri"
  }

  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val startTime = LocalDateTime.now()

    nextFilter(requestHeader).map { result =>

      if (requestHeader.path.contains("metrics"))
        result
      else {
        val loggingData = RequestLogInfo(
          startTime,
          requestHeader.uri,
          requestHeader.method,
          requestHeader.secure,
          requestHeader.headers.toMap,
          requestHeader.queryString,
          requestHeader.rawQueryString
        )
        //log
        log(loggingData, result)
        // leave unchanged
        result
      }

    }
  }

  private def log(loggingData: RequestLogInfo, result: Result): Unit = {
    doLogging(loggingData, result, LocalDateTime.now())
  }

  private def doLogging(logInfo: RequestLogInfo, result: Result, endTime: LocalDateTime) = {

    val markers: LogstashMarker = appendEntries(Map(
      "path" -> logInfo.uri,
      "httpMethod" -> logInfo.method,
      "startTime" -> logInfo.startTime.format(dateTimeFormatter),
      "queryParams" -> requestHeadersToString(logInfo.queryParameters),
      "httpRequestHeaders" -> requestHeadersToString(logInfo.headers),
      "httpCode" -> result.header.status,
      "httpResponseHeaders" -> responseHeaderstoString(result.header.headers),
      "reponseDuration" -> logInfo.startTime.until(endTime, ChronoUnit.MILLIS)
    ).asJava)

    logger.info(markers, logInfo.toString)
  }

  private def requestHeadersToString(headers: Map[String, Seq[String]]): String = {
    headers.map {
      case (key, values) =>
        s"$key: ${values.mkString("|")}"
    }.mkString(",")
  }

  private def responseHeaderstoString(headers: Map[String, String]): String = {
    headers.map {
      case (key, value) =>
        s"$key: $value"
    }.mkString(",")
  }

}

class Filters @Inject() (gzip: GzipFilter, metrics: MetricsFilter, access: AccessLogFilter)
  extends DefaultHttpFilters(access, gzip, metrics)
