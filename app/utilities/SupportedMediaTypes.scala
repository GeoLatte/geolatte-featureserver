package utilities

import config.Constants.{ Format, Version }
import play.api.http.MediaType
import play.api.mvc.RequestHeader

object SupportedMediaTypes {

  object mediaSubType {

    private val mediaTypeSubTypePrefix = "vnd.geolatte-featureserver+"

    val supported: Set[String] = (
      for { f <- Format.values } yield mediaTypeSubTypePrefix + Format.stringify(f)
    ) + "json"

    def apply(format: Format.Value): String = mediaTypeSubTypePrefix + Format.stringify(format)

    def unapply(mediaSubType: String): Option[Format.Value] = supported.find(_ == mediaSubType) match {
      case Some("json") => Some(Format.JSON)
      case Some(smt) => Format.unapply(smt.stripPrefix(mediaTypeSubTypePrefix))
      case _ => None
    }

  }

  def apply(format: Format.Value, version: Version.Value = Version.default): MediaType =
    new MediaType("application", mediaSubType(format), Seq(("version", Some(Version.stringify(version)))))

  def unapply(mediatype: MediaType): Option[(Format.Value, Version.Value)] = {

    val formatOpt = (mediatype.mediaType, mediatype.mediaSubType) match {
      case ("*", "*") | ("application", "*") => Some(Format.JSON)
      case ("application", "json") => Some(Format.JSON)
      case ("application", mediaSubType(format)) => Some(format)
      case _ => None
    }

    val versionOpt = mediatype.parameters.find(_._1 == "version").flatMap(_._2) match {
      case Some(Version(version)) => Some(version)
      case Some(_) => None
      case None => Some(Version.default)
    }

    (formatOpt, versionOpt) match {
      case (Some(f), Some(v)) => Some((f, v))
      case _ => None
    }
  }

  /**
   * Extractor that extracts format and version
   * @param header the RequestHeader from which to extract the Accepted format and versions
   * @return
   */
  def unapply(header: RequestHeader): Option[(Format.Value, Version.Value)] = header.acceptedTypes.flatMap {
    case SupportedMediaTypes(format, version) => Some(format, version)
    case _ => None
  }.headOption

}