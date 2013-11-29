package integration

import org.specs2.mutable.Specification
import play.api.test.FakeApplication

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/15/13
 */
class ViewsAPISpecs extends Specification {

  "The application" should {
    val testDbName = "xfstestdb"
    val testColName = "xfstestcoll"

    val fakeApplication = FakeApplication()

    "On a GET db/col/views, the defined views should be returned" in {
      import RestApiDriver._

      onCollection(testDbName, testColName, fakeApplication) {
        pending
      }

    }

  }



}
