package utilities

import play.api.libs.json._

import scala.util.Try

object JsonUtils {

  def toJsValue(value: Any): JsValue = value match {
    case v if v == null => JsNull
    case v: String      => JsString(v)
    case v: Int         => JsNumber(v)
    case v: Long        => JsNumber(v)
    case b: Boolean     => JsBoolean(b)
    case f: Float       => JsNumber(BigDecimal(f.toDouble))
    case d: Double      => JsNumber(BigDecimal(d))
    case b: BigDecimal  => JsNumber(b)
    case e => Utils.withTrace(s"Converting value $e (${e.getClass.getCanonicalName}) by toString method")(
      Try { JsString(e.toString) }.toOption.getOrElse(JsNull)
    )
  }

  def toJson(text: String)(implicit reads: Reads[JsObject]): Option[JsObject] =
    Json
      .parse(text)
      .asOpt[JsObject](reads)
}
