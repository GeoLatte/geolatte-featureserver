package controllers

import org.specs2.mutable.Specification
import play.api.http.Status._
import play.api.mvc.Result

class ExceptionHandlersSpec extends Specification with ExceptionHandlers {

  private def statusOf(r: Result): Int = r.header.status
  private def bodyOf(r: Result): String = {
    import org.apache.pekko.stream.Materializer
    import org.apache.pekko.util.ByteString
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val mat: Materializer = Materializer.matFromSystem(org.apache.pekko.actor.ActorSystem("test-mat"))
    val bytes: ByteString = Await.result(r.body.consumeData, 5.seconds)
    bytes.utf8String
  }

  "commonExceptionHandler" should {

    "return a generic 500 response without leaking exception details" in {
      val result = commonExceptionHandler.apply(new RuntimeException("sensitive backend detail"))
      statusOf(result) must_=== INTERNAL_SERVER_ERROR
      bodyOf(result) must_=== "Internal server error."
    }
  }
}
