package persistence.postgresql

import Exceptions.InvalidQueryException
import org.geolatte.geom.codec.Wkt

import scala.util.{ Failure, Try }

object PGSqlUtils {
  def safeQuotes(value: String): String = value.replace("'", "''")
  def safeLiteralString(value: String): String = s"'${safeQuotes(value)}'"

  /**
   * Validate a WKT/EWKT string for safe inclusion in a SQL literal.
   *
   * Two independent checks, neither of which depends on parser leniency:
   *
   *   1. Character allowlist — WKT/EWKT only contains letters, digits,
   *      `=`, `;`, `(`, `)`, `,`, `.`, `+`, `-`, and whitespace. Anything
   *      else (especially `'` and `--`) cannot appear in valid WKT and is
   *      rejected immediately. This is the security gate: it is impossible
   *      to construct a SQL injection from these characters inside a
   *      string literal produced by `safeLiteralString`.
   *
   * 2. Structural parse — `Wkt.fromWkt` confirms the input is
   * syntactically valid WKT. The parsed Geometry is discarded; we
   * return the original string (via `safeLiteralString`) so that
   * dialect-specific formatting (Z/ZM markers, SRID prefix, spacing)
   * is preserved exactly. No round-trip through `Wkt.toWkt`.
   */
  def safeWkt(wkt: String): String = {
    if (!isWktSafe(wkt))
      throw InvalidQueryException(s"Invalid WKT geometry: $wkt")
    Try(Wkt.fromWkt(wkt)).recoverWith {
      case e => Failure(InvalidQueryException(s"Invalid WKT geometry: $wkt — ${e.getMessage}"))
    }.get
    safeLiteralString(wkt)
  }

  // Characters that can appear in WKT/EWKT: letters (POINT, LINESTRING,
  // EMPTY, SRID, Z, ZM, ...), digits, the SRID key-value separator `=`,
  // the SRID terminator `;`, coordinate-list delimiters `(`, `)`, `,`,
  // decimal point `.`, signs `+`/`-`, and whitespace.
  private def isWktSafe(wkt: String): Boolean =
    wkt.nonEmpty && wkt.forall(c =>
      c.isLetterOrDigit || "=;(),.+- \t\n\r".indexOf(c) >= 0
    )

  def safeIdentifier(name: String): String = s""""${escapeIdentifier(name)}""""

  private def escapeIdentifier(name: String): String = name.replace("\"", "\"\"")
}
