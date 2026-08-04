package persistence.postgresql

import Exceptions.InvalidQueryException
import org.geolatte.geom.codec.Wkt

import scala.util.{ Failure, Try }

object PGSqlUtils {
  def safeQuotes(value: String): String = value.replace("'", "''")
  def safeLiteralString(value: String): String = s"'${safeQuotes(value)}'"

  /**
   * Quote a string for use as a PostgreSQL jsonpath string literal.
   *
   * This escapes the jsonpath/JSON string layer only. The complete jsonpath
   * expression must still be passed through `safeLiteralString` before being
   * embedded in SQL.
   *
   * JSON requires an escape for exactly three things — the closing quote, the
   * backslash itself, and the control characters — so the cases below are the
   * complete set. Everything else, `/` and non-ASCII included, is legal
   * unescaped and is left alone.
   *
   * Escaping the backslash is not only a matter of not ending the string early:
   * PostgreSQL's jsonpath lexer accepts unknown escapes and silently drops the
   * backslash, so an unescaped `\d` from a user would reach the value as a bare
   * `d`.
   */
  def safeJsonpathString(value: String): String = {
    val escaped = new StringBuilder(value.length)
    value.foreach {
      case '"'  => escaped.append("\\\"")
      case '\\' => escaped.append("\\\\")
      // The five named forms are the same characters the fallback below would
      // catch anyway; they only keep the generated SQL readable in logs.
      case '\b' => escaped.append("\\b")
      case '\f' => escaped.append("\\f")
      case '\n' => escaped.append("\\n")
      case '\r' => escaped.append("\\r")
      case '\t' => escaped.append("\\t")
      // The remaining control characters — think NUL, ESC, vertical tab. They
      // have no printable shape and no shorthand of their own, so JSON wants
      // them written as a backslash, then a u, then four hex digits (the code
      // point 1 becomes a six-character escape).
      case c if c < FirstPrintableChar => escaped.append("\\u%04x".format(c.toInt))
      case c                           => escaped.append(c)
    }
    s""""$escaped""""
  }

  /**
   * The space, the lowest character with a printable form. Everything below it
   * is a control character, which is exactly the set JSON refuses to carry raw
   * inside a string.
   */
  private val FirstPrintableChar = ' '

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
