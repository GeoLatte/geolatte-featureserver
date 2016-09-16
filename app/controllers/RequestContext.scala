package controllers

import config.Constants.{ Format, Version }
import play.api.http.MediaType
import play.api.mvc.RequestHeader

case class RequestContext(
    request: RequestHeader,
    format: Option[Format.Value] = None,
    filename: Option[String] = None,
    version: Option[Version.Value] = None
) {
  lazy val mediaType: (MediaType, Format.Value) = {
    val (fmt, v) = (format, version, request) match {
      case (Some(f), Some(ve), _) => (f, ve)
      case (Some(f), None, _) => (f, Version.default)
      case (_, _, SupportedMediaTypes(f, ve)) => (f, ve)
      case _ => (Format.JSON, Version.default)
    }
    (SupportedMediaTypes(fmt, v), fmt)
  }

  def sep(implicit req: RequestHeader) = {
    //TODO -- restructure queryparam extraction
    val SEP = QueryParam("sep", (s: String) => Some(s))
    SEP.extract(req.queryString).filterNot(_.isEmpty).getOrElse(",")
  }

}