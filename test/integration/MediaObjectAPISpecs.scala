package integration

import org.specs2.mutable.Specification
import play.api.libs.json.Json
import play.api.test.Helpers._
import scala.Some
import play.api.test.FakeApplication

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/26/13
 */
class MediaObjectAPISpecs extends Specification {


  "The application" should {
    val testDbName = "xfstestdb"
    val testColName = "xfstestcoll"

    val fakeApplication = FakeApplication()

    "On a PUT/DELETE of a media object, the media is created/deleted" in {
      import RestApiDriver._

      val imageInbase64 = scala.io.Source.fromFile("test/resources/cci.base64").mkString

      onCollection(testDbName, testColName, fakeApplication) {

        val putResult = postMediaObject(testDbName, testColName, Json.obj(
          "content-type" -> "application/octet-stream",
          "name" -> "creative-commons",
          "data" -> imageInbase64)
        )
        val urlOpt = (putResult.responseBody.getOrElse(Json.obj()) \ "url").asOpt[String]

        urlOpt match {
          case None => failure
          case Some(url) => {
            println("returned URL: "  + url)
            val getResult = getMediaObject(url)
            val deleteResult = deleteMediaObject(url)
            (putResult.status must equalTo(OK)) and
              (getResult.status must equalTo(OK)) and
              (deleteResult.status must equalTo(OK))
          }
        }

      }
    }
  }

}
