import config.AppExecutionContexts
import play.api._
import mvc._
import mvc.Results._
import scala.concurrent.Future

object Global extends GlobalSettings {


  override def onError(request: RequestHeader, ex: Throwable) = {
    val useful = ex match {
      case e: UsefulException => e
      case _ => UnexpectedException(unexpected = Some(ex))
    }
    import AppExecutionContexts.streamContext
    Future { InternalServerError(views.html.defaultpages.error(useful)) }
  }

  override def onHandlerNotFound(request: RequestHeader): Future[SimpleResult] = {
    import AppExecutionContexts.streamContext
    Future { NotFound(s"Request ${request.path} not found.") }
  }

}
