package nosql

import kamon.Kamon
import metrics.RequestMetrics
import org.slf4j.LoggerFactory
import play.api._
import play.api.mvc.Results._
import play.api.mvc._

import scala.concurrent.Future

object Global extends GlobalSettings {


  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  override def onError(request: RequestHeader, ex: Throwable) = {
    val useful = ex match {
      case e: UsefulException => e
      case _ => UnexpectedException(unexpected = Some(ex))
    }
    Future { InternalServerError(views.html.defaultpages.error(useful)) }
  }

  override def onHandlerNotFound(request: RequestHeader): Future[Result] = {
    Future { NotFound(s"Request ${request.path} not found.") }
  }

  override def onStart(app: Application) {
      Kamon.start()
  }

  val requestLogger = LoggerFactory.getLogger("requests")


  val loggingFilter = Filter { (nextFilter, requestHeader) =>
    val startTime = System.currentTimeMillis

    nextFilter(requestHeader).map { result =>
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime

      val metrics = Kamon.metrics.entity(RequestMetrics, "featureserver-request")
      metrics.requests.increment()
      metrics.requestExecutionTime.record(requestTime)

      val myCounter = Kamon.metrics.counter("my-counter")
      myCounter.increment()

      requestLogger.info(s"${requestHeader.method} ${requestHeader.uri} ; $requestTime ; ${result.header.status}")
      result.withHeaders("Request-Time" -> requestTime.toString)

    }
  }

  override def onStop(app: Application): Unit = {
    Kamon.shutdown()
    //TODO also stop repository connection pools (e.g. postgresql-async!)
  }


}



