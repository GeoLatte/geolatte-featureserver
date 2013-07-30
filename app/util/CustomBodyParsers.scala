package util

import scala.language.reflectiveCalls
import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.iteratee._
import play.api.libs.iteratee.Input._


object CustomBodyParsers {

  lazy val DEFAULT_MAX_TEXT_LENGTH: Int = Play.maybeApplication.flatMap { app =>
        app.configuration.getBytes("parsers.text.maxLength").map(_.toInt)
      }.getOrElse(1024 * 100)

  /**
   * Parse the body as Json without checking the Content-Type, and also
   *
   *
   * @param maxLength Max length allowed or returns EntityTooLarge HTTP response.
   */
  def tolerantNullableJson(maxLength: Int): BodyParser[JsValue] = BodyParser("json, maxLength=" + maxLength) {
    request =>
      Traversable.takeUpTo[Array[Byte]](maxLength).apply(Iteratee.consume[Array[Byte]]().map {
        bytes => scala.util.control.Exception.allCatch[JsValue].either {
            if (bytes.length == 0) JsNull
            else Json.parse(new String(bytes, request.charset.getOrElse("utf-8")))
          }.left.map {
            e =>
              (Play.maybeApplication.map(_.global.onBadRequest(request, "Invalid Json")).getOrElse(Results.BadRequest), bytes)
          }
      }).flatMap( Iteratee.eofOrElse(Results.EntityTooLarge) )
        .flatMap {
        case Left(b) => Done(Left(b), Empty)
        case Right(it) => it.flatMap {
          case Left((r, in)) => Done(Left(r), El(in))
          case Right(json) => Done(Right(json), Empty)
        }
      }
  }

  /**
   * Parse the body as Json without checking the Content-Type, and allow null values
   */
  def tolerantNullableJson: BodyParser[JsValue] = tolerantNullableJson(DEFAULT_MAX_TEXT_LENGTH)

}

