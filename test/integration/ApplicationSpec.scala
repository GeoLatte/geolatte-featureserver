package integration

import org.specs2.mutable._

import play.api.test._
import play.api.test.Helpers._
import play.api.libs.ws.WS
import play.api.libs.json._
import UtilityMethods._

/**
 * API Spec
 *
 */
class ApplicationSpec extends Specification {

  "The application" should {

  "send 404 on a bad request" in new WithServer {
      await(WS.url("http://localhost:" + port + "/foo").get).status must equalTo(NOT_FOUND)
    }

    "redirect to the index page" in {
      running(FakeApplication()) {
        val home = route(FakeRequest(GET, "/")).get

        status(home) must equalTo(SEE_OTHER)
        header("Location", home) must beSome("/index.html")
      }
    }

  }

}