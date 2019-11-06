import play.api.{ Logger, UnexpectedException, UsefulException }
import play.api.http.{ HttpErrorHandler, Status => StatusCodes }
import play.api.mvc.Results._
import play.api.mvc.{ RequestHeader, Result }
import utilities.Utils

import scala.concurrent.Future

/**
 * Created by Karel Maesen, Geovise BVBA on 09/09/16.
 */
class ErrorHandler extends HttpErrorHandler {

  override def onClientError(request: RequestHeader, statusCode: Int, message: String): Future[Result] = {

    import StatusCodes._

    statusCode match {
      case NOT_FOUND => Utils.withWarning(s"Request $request not found.") {
        Future.successful(NotFound(s"Request $request not found."))
      }
      case _ => Utils.withWarning(s"Request $request failed with code $statusCode: $message") {
        Future.successful(
          Status(statusCode)("A client error occurred: " + message))
      }
    }

  }

  override def onServerError(request: RequestHeader, exception: Throwable): Future[Result] = {

    val useful = exception match {
      case e: UsefulException => e
      case _ => UnexpectedException(unexpected = Some(exception))
    }
    Logger.error(s"Error on request $request", useful)
    Future.successful { InternalServerError(s"A server error occured: $useful") }
  }
}
