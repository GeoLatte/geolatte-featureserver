package utilities

import config.ConfigurationValues
import org.specs2.mutable._
import config.ConfigurationValues.{Version, Format}
import play.api.http.{MediaRange, MediaType}
import play.api.test.{FakeRequest, FakeHeaders}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/23/13
 */
class SupportedMediaTypeSpec extends Specification {

  def mkMediatype(mediatype: String, version: String = null) = if (version != null) mediatype + "; otherprop=1; version=\""  + version + "\";q=0.9" else s"$mediatype"

  def mkHeader(mediatypeWithProps: String) = new FakeHeaders(List(("Accept", List("text/plain;q=0.8, " + mediatypeWithProps))))

  def mkRequest(mediatype: String, version: String = null) = {
    val mt : String = mkMediatype(mediatype, version)
    new FakeRequest[String]("GET", "/databases", mkHeader(mt), "")
  }

  def specDescription[T](mediatype: String, specifiedVersion: String, expected: Option[(Format.Value, Version.Value)] ) = {
    val mt = mkMediatype(mediatype, specifiedVersion)
    expected match {
      case Some((format, version)) => s"matches a $mt with format = ${format.toString} and version = ${Version.stringify(version)}"
      case None => s"doesn't match a $mt"
    }

  }

  /**
   * Generates a spec for the VersionAwareAccepting class
   * @param mediatype mediatype (without version paramater) to match
   * @param specifiedVersion the specified version to add to the mediatype (if null no version parameter is added)
   * @param expected None if mediatype and specifiedVersion should not match, else some(format,version) that is expected to be returned
   * @return
   */
  def satisfySpec(mediatype: String, specifiedVersion: String, expected : Option[(Format.Value, Version.Value)] ) = {
    val matcher = expected match {
      case Some((format, version)) => beSome(format, version)
      case None => beNone
    }
    specDescription(mediatype, specifiedVersion, expected) in {
      val req = mkRequest(mediatype, specifiedVersion)
      val res = req match {
        case SupportedMediaTypes(f, v) => Some(f,v)
        case _ => None
      }
      res must matcher
    }
  }

  "The VersionAwareAccepting " should {
    satisfySpec("application/vnd.geolatte-featureserver+json", "1.0", Some( Format.JSON -> Version.v1_0) )
    satisfySpec("application/vnd.geolatte-featureserver+json", null, Some(Format.JSON -> Version.v1_0))
    satisfySpec("application/json", null, Some(Format.JSON -> Version.v1_0))
    satisfySpec("application/json", "1.0", Some(Format.JSON -> Version.v1_0))
    satisfySpec("application/vnd.geolatte-featureserver+csv", "1.0", Some(Format.CSV -> Version.v1_0))
    satisfySpec("application/vnd.geolatte-featureserver+csv", null, Some(Format.CSV -> Version.v1_0))

    satisfySpec("application/text", "1.0", None )
    satisfySpec("application/text", null, None )

    satisfySpec("application/vnd.geolatte-featureserver+json", "1.1", None )
    satisfySpec("application/vnd.geolatte-featureserver+csv", "1.1", None )
    satisfySpec("application/vnd.geolatte-featureserver+txt", "1.0", None )
    satisfySpec("application/vnd.geolatte-featureserver+txt", null, None )

    satisfySpec("*/*", null, Some( Format.JSON -> Version.v1_0) )
    satisfySpec("application/*", null, Some( Format.JSON -> Version.v1_0) )


    val mt : MediaType = SupportedMediaTypes(Format.JSON,  Version.default)


    val test = mt match {
      case SupportedMediaTypes(fmt, version) =>
        fmt must_== Format.JSON
        version must_== Version.default
      case _ => failure
    }

  }


}
