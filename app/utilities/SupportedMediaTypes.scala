package utilities


import config.ConfigurationValues.{Version, Format}
import play.api.Logger
import play.api.http.MediaRange
import play.api.mvc.RequestHeader


object SupportedMediaTypes {

  private val GeolatteSubTypeRegex ="vnd.geolatte-featureserver\\+([a-zA-Z]+)".r
  private val VersionParamRegex = ".*version=\"?([0-9.]+)\"?.*".r

  def apply(format : Format.Value, version : Version.Value = Version.default) = (format, version) match {
    case (f : Format.Value, v : Version.Value) => "vnd.geolatte-featureserver+" + Format.stringify(format) + ";version=\"" + Version.stringify(v) + "\""
    case (f : Format.Value,_) => "vnd.geolatte-featureserver+" + Format.stringify(format)
    case (_, _) => throw new MatchError()
  }

  def unapply( mediaRange: MediaRange ) : Option[(Format.Value, Version.Value)] = {


    val formatOpt = (mediaRange.mediaType, mediaRange.mediaSubType) match {
      case ("*", "*") | ("application", "*") => Some(Format.JSON)
      case ("application",  "json") => Some(Format.JSON)
      case ("application", GeolatteSubTypeRegex(Format(format))) => Some(format)
      case _ => None
    }

    val versionOpt = mediaRange.parameters.find(_._1 == "version").flatMap( _._2) match {
      case Some(Version(version)) => Some(version)
      case Some(_) => None
      case None => Some(Version.default)
    }

    (formatOpt, versionOpt) match {
      case (Some(f), Some(v)) => Some((f,v))
      case _ => None
    }
  }

  /**
     * Extractor that extracts format and version
     * @param header the RequestHeader from which to extract the Accepted format and versions
     * @return
     */
  def unapply(header: RequestHeader): Option[(Format.Value, Version.Value)] = header.acceptedTypes.map {
    case SupportedMediaTypes(format, version) => Some(format, version)
    case _ => None
  }.flatten.headOption

}