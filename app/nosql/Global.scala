package nosql

import com.codahale.metrics.{Slf4jReporter, JmxReporter, MetricRegistry}
import config.AppExecutionContexts
import nl.grons.metrics.scala.InstrumentedBuilder
import play.api._
import mvc._
import play.filters.gzip.GzipFilter
import mvc.Results._
import scala.concurrent.Future
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

object Global extends GlobalSettings {


  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  override def onError(request: RequestHeader, ex: Throwable) = {
    val useful = ex match {
      case e: UsefulException => e
      case _ => UnexpectedException(unexpected = Some(ex))
    }
    Future { InternalServerError(views.html.defaultpages.error(useful)) }
  }

  override def onHandlerNotFound(request: RequestHeader): Future[SimpleResult] = {
    Future { NotFound(s"Request ${request.path} not found.") }
  }

  val metricRegistry = new MetricRegistry()

  val jmxReporter = JmxReporter.forRegistry(metricRegistry).build()

  val logReporter = Slf4jReporter.forRegistry(metricRegistry)
    .outputTo(LoggerFactory.getLogger("metrics"))
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()

  override def onStart(app: Application) {

    jmxReporter.start()
    logReporter.start(1, TimeUnit.MINUTES)
  }

  val requestLogger = LoggerFactory.getLogger("requests")

  val loggingFilter = Filter { (nextFilter, requestHeader) =>
    val startTime = System.currentTimeMillis

    nextFilter(requestHeader).map { result =>
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime
      val timer = metricRegistry.timer(s"requests.${requestHeader.path.tail.replaceAll("/", ".")}.${requestHeader.method.toLowerCase}")
      timer.update(requestTime, TimeUnit.MILLISECONDS)
      requestLogger.info(s"${requestHeader.method} ${requestHeader.uri} ; ${requestTime} ; ${result.header.status}")
      result.withHeaders("Request-Time" -> requestTime.toString)

    }
  }

  override def onStop(app: Application) {
    logReporter.stop()
    jmxReporter.stop()
  }


  override def doFilter(next: EssentialAction) : EssentialAction = {
    Filters(super.doFilter(next), loggingFilter, new GzipFilter())
  }

}



