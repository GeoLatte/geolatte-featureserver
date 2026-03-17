package persistence.postgresql

import Exceptions.InvalidQueryException
import org.geolatte.geom.codec.Wkt

import scala.util.{ Failure, Success, Try }

object PGSqlUtils {
  def safeQuotes(value: String): String = value.replace("'", "''")
  def safeLiteralString(value: String): String = s"'${safeQuotes(value)}'"
  def safeWkt(wkt: String): String = Try(Wkt.toWkt(Wkt.fromWkt(wkt))) match {
    case Success(safe) => safeLiteralString(safe)
    case Failure(_)    => throw InvalidQueryException(s"Invalid WKT geometry: $wkt")
  }
  def safeIdentifier(name: String): String = s""""${escapeIdentifier(name)}""""

  private def escapeIdentifier(name: String): String = name.replace("\"", "\"\"")
}
