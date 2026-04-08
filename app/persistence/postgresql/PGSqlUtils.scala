package persistence.postgresql

import Exceptions.InvalidQueryException
import org.geolatte.geom.codec.Wkt

import scala.util.{ Failure, Success, Try }

object PGSqlUtils {
  private val DimensionMarker = "(?i)\\s+ZM?(?=\\s*\\()".r

  def safeQuotes(value: String): String = value.replace("'", "''")
  def safeLiteralString(value: String): String = s"'${safeQuotes(value)}'"
  def safeWkt(wkt: String): String =
    validate(wkt)
      .orElse(retryWithoutDimensionMarker(wkt))
      .map(_ => safeLiteralString(wkt))
      .getOrElse(throw InvalidQueryException(s"Invalid WKT geometry: $wkt"))
  def safeIdentifier(name: String): String = s""""${escapeIdentifier(name)}""""

  private def validate(wkt: String): Try[Unit] =
    Try(Wkt.fromWkt(wkt)).map(_ => ())

  /** Try to validate the WKT after removing standard Z/ZM dimension markers that geolatte-geom 0.14 does not understand. */
  private def retryWithoutDimensionMarker(wkt: String): Try[Unit] = {
    val normalized = DimensionMarker.replaceAllIn(wkt, "")
    if (normalized == wkt) Failure(InvalidQueryException(s"Invalid WKT geometry: $wkt"))
    else validate(normalized)
  }

  private def escapeIdentifier(name: String): String = name.replace("\"", "\"\"")
}
