import play.api._
import mvc._
import mvc.Results._

object Global extends GlobalSettings{


  override def onError(request: RequestHeader, ex: Throwable) = {
      InternalServerError( ex.getStackTraceString )
    }

  override def onHandlerNotFound(request: RequestHeader): Result = {
    NotFound(s"Request ${request.path} not found.")
  }

}
