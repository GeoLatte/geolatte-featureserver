package persistence.postgresql

import org.specs2.mutable.Specification

class PGSqlUtilsSpec extends Specification {
  private def preservesOriginalWkt(wkt: String) =
    PGSqlUtils.safeWkt(wkt) === s"'$wkt'"

  "PGSqlUtils.safeWkt" should {
    "canonicalize WKT that GeoLatte can decode" in {
      PGSqlUtils.safeWkt(
        "SRID=31370;POINT(10 12)"
      ) === "'SRID=31370;POINT(10 12)'"
    }

    "reject a quoted SQL injection payload in WKT input" in {
      PGSqlUtils.safeWkt("SRID=4326;POINT(10 10)') OR TRUE --") must throwA[Exceptions.InvalidQueryException]
    }

    "keep original WKT for standard Z and ZM variants after fallback validation" in {
      val examples = Seq(
        "POINT Z (1 2 3)",
        "POINT ZM (1 2 4 5)",
        "MULTIPOINT Z ((1 2 5),(3 4 6))",
        "MULTIPOINT ZM ((1 2 5 6),(3 4 7 8))",
        "LINESTRING Z (30 10 1,10 30 2,40 40 3)",
        "LINESTRING ZM (30 10 1 4,10 30 2 5,40 40 3 6)",
        "MULTILINESTRING Z ((10 10 0,20 20 1,10 40 2),(40 40 3,30 30 4,40 20 5,30 10 6))",
        "MULTILINESTRING ZM ((10 10 0 100,20 20 1 101,10 40 2 102),(40 40 3 103,30 30 4 104,40 20 5 105,30 10 6 106))",
        "POLYGON Z ((30 10 0,40 40 1,20 40 2,10 20 1,30 10 0))",
        "POLYGON ZM ((30 10 0 1,40 40 1 2,20 40 2 3,10 20 1 4,30 10 0 1))",
        "MULTIPOLYGON Z (((40 40 1,20 45 2,45 30 3,40 40 1)),((20 35 1,10 30 2,10 10 3,30 5 4,45 20 5,20 35 1),(30 20 1,20 15 2,20 25 3,30 20 1)))",
        "MULTIPOLYGON ZM (((40 40 1 1,20 45 2 3,45 30 3 4,40 40 1 1)),((20 35 1 1,10 30 2 2,10 10 3 4,30 5 4 5,45 20 5 6,20 35 1 1),(30 20 1 1,20 15 2 2,20 25 3 3,30 20 1 1)))",
        "GEOMETRYCOLLECTION Z (POINT Z (40 10 1),LINESTRING Z (10 10 1,20 20 2,10 40 3),POLYGON Z ((40 40 1,20 45 2,45 30 3,40 40 1)))",
        "GEOMETRYCOLLECTION ZM (POINT ZM (40 10 1 2),LINESTRING ZM (10 10 1 2,20 20 2 3,10 40 3 4),POLYGON ZM ((40 40 1 1,20 45 2 2,45 30 3 3,40 40 1 1)))",

        "SRID=31370;MULTIPOLYGON ZM(((118901.556 180399.729 0 0,118922.556 180399.729 0 0,118922.556 180420.729 0 0,118901.556 180399.729 0 0)))"
      )

      examples
        .map(preservesOriginalWkt)
        .reduce(_ and _)
    }
  }

  "PGSqlUtils.safeJsonpathString" should {
    "escape JSON string delimiters, backslashes and control characters" in {
      PGSqlUtils.safeJsonpathString("a\"b\\c\n") === "\"a\\\"b\\\\c\\n\""
    }

    // U+0001 built from its code point: the character is invisible in an
    // editor, and a source-level \u escape is resolved by the Scala lexer
    // before the string ever exists.
    "escape a control character that has no named JSON escape" in {
      PGSqlUtils.safeJsonpathString("a" + 1.toChar + "b") === "\"a\\u0001b\""
    }

    "leave characters JSON does not require escaping untouched" in {
      PGSqlUtils.safeJsonpathString("categorieën a/b") === "\"categorieën a/b\""
    }
  }
}
