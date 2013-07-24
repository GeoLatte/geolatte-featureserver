import play.api._
import mvc._
import mvc.Results._

object Global extends GlobalSettings {


  override def onError(request: RequestHeader, ex: Throwable) = {
    val useful = ex match {
      case e: UsefulException => e
      case _ => UnexpectedException(unexpected = Some(ex))
    }
    InternalServerError(
      views.html.defaultpages.error(useful)
    )
  }

  override def onHandlerNotFound(request: RequestHeader): Result = {
    NotFound(s"Request ${request.path} not found.")
  }

}
