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

    import UtilityMethods._

    "redirect to the index page" in {
      running(FakeApplication()) {
        val home = route(FakeRequest(GET, "/")).get

        status(home) must equalTo(SEE_OTHER)
        header("Location", home) must beSome("/index.html")
      }
    }

  }

}