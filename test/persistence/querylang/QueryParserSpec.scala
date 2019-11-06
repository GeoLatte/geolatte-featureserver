package persistence.querylang

import org.specs2.matcher.ValueCheck._
import org.specs2.mutable.Specification

/**
 * Created by Karel Maesen, Geovise BVBA on 15/01/15.
 */
class QueryParserSpec extends Specification {

  "A QueryParser " should {

    "parse equality expressions correctly " in {
      QueryParser.parse("var =  '123' ") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123")))
    }

    "parse greater or equals expressions correctly " in {
      QueryParser.parse("var >=  '123' ") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(PropertyExpr("var"), GTE, LiteralString("123")))
    }

    "handle parentesis properly" in {
      QueryParser.parse("( var =  '123' )") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123")))
    }

    "Interpret literal numbers  properly" in {

      QueryParser.parse(" var =  -123 ") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(-123)))) and (QueryParser.parse("var =  123.54") must beSuccessfulTry[BooleanExpr].withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54))))) and
          (QueryParser.parse(" var =  123.54E3 ") must beSuccessfulTry[BooleanExpr].withValue(
            ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E3))))) and (QueryParser.parse(" var =  123.54E-3") must beSuccessfulTry[BooleanExpr].withValue(
              ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E-3)))))

    }

    "Interpret literals strings" in {

      (
        QueryParser.parse(" var =  'abc.d' ") must beSuccessfulTry[BooleanExpr].withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("abc.d")))) and (
            QueryParser.parse("var =  '!abc._-$%^&*()@#$%%^_+00==\"'") must beSuccessfulTry[BooleanExpr].withValue(
              ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("!abc._-$%^&*()@#$%%^_+00==\"")))) and (
                QueryParser.parse(" var =  'abc''def' ") must beSuccessfulTry[BooleanExpr].withValue(
                  ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("abc'def")))) and (
                    QueryParser.parse(""" var =  '{"ab" : "def"}' """) must beSuccessfulTry[BooleanExpr].withValue(
                      ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("""{"ab" : "def"}"""))))

    }

    "handle negations properly" in {

      (
        QueryParser.parse(" not (var =  '123')") must beSuccessfulTry[BooleanExpr].withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123"))))) and (
            QueryParser.parse(" not var =  '123'") must beSuccessfulTry[BooleanExpr].withValue(
              BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123")))))

    }

    "handle AND combinator properly" in {

      (
        QueryParser.parse("(vara > 12) and not (varb =  '123')") must beSuccessfulTry[BooleanExpr].withValue(
          BooleanAnd(ComparisonPredicate(
            PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))), BooleanNot(
            ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123")))))) and (
            QueryParser.parse("vara > 12 and varb =  '123'") must beSuccessfulTry[BooleanExpr].withValue(BooleanAnd(
              ComparisonPredicate(
                PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
              ComparisonPredicate(
                PropertyExpr("varb"), EQ, LiteralString("123"))))) and (
              QueryParser.parse("vara > 12 AND varb =  '123'") must beSuccessfulTry[BooleanExpr].withValue(BooleanAnd(
                ComparisonPredicate(
                  PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
                ComparisonPredicate(
                  PropertyExpr("varb"), EQ, LiteralString("123")))))

    }

    "handle consecutive AND combinator without needing nesting parenthesis" in {
      QueryParser.parse("vara > 12 and varb =  '123' and varc = 'bla'") must beSuccessfulTry[BooleanExpr].withValue(
        BooleanAnd(
          BooleanAnd(
            ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
            ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))),
          ComparisonPredicate(PropertyExpr("varc"), EQ, LiteralString("bla"))))
    }

    "handle OR combinator properly" in {
      (
        QueryParser.parse("(vara > 12) or not (varb =  '123')") must beSuccessfulTry[BooleanExpr].withValue(
          BooleanOr(
            ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
            BooleanNot(ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123")))))) and (
            QueryParser.parse("vara > 12 or varb =  '123'") must beSuccessfulTry[BooleanExpr].withValue(
              BooleanOr(
                ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
                ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))))) and (
                QueryParser.parse("vara > 12 or ( varb =  '123' and varc = 'abc' ) ") must beSuccessfulTry[BooleanExpr]
                .withValue(
                  BooleanOr(
                    ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
                    BooleanAnd(
                      ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123")),
                      ComparisonPredicate(PropertyExpr("varc"), EQ, LiteralString("abc"))))))
    }

    "handle consecutive OR combinator without needing nesting parenthesis" in {
      QueryParser.parse("vara > 12 or varb =  '123' or varc = 'bla'") must beSuccessfulTry[BooleanExpr].withValue(
        BooleanOr(
          BooleanOr(
            ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
            ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))),
          ComparisonPredicate(PropertyExpr("varc"), EQ, LiteralString("bla"))))
    }

    "handle boolean literal values in expression" in {
      (
        QueryParser.parse(" not (var =  true)") must beSuccessfulTry[BooleanExpr].withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralBoolean(true))))) and (
            QueryParser.parse(" not var =  false") must beSuccessfulTry[BooleanExpr].withValue(
              BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralBoolean(false)))))
    }

    "treat a boolean literal as a valid expression" in {
      (
        QueryParser.parse("true") must beSuccessfulTry[BooleanExpr].withValue(
          LiteralBoolean(true))) and (
            QueryParser.parse(" not (false)") must beSuccessfulTry[BooleanExpr].withValue(
              BooleanNot(LiteralBoolean(false))))
    }

    "supports the in operator" in {
      (QueryParser.parse("var in ('a', 'b', 'c')") must beSuccessfulTry[BooleanExpr].withValue(
        InPredicate(PropertyExpr("var"), ValueListExpr(List(
          LiteralString("c"),
          LiteralString("b"),
          LiteralString("a")))))) and (QueryParser.parse("var in  (  1,2, 3 )") must beSuccessfulTry[BooleanExpr].withValue(
          InPredicate(PropertyExpr("var"), ValueListExpr(List(LiteralNumber(3), LiteralNumber(2), LiteralNumber(
            1))))))
    }

    "support regex predicates" in {
      QueryParser.parse("""var ~ /a\.*.*b/""") must beSuccessfulTry[BooleanExpr].withValue(
        RegexPredicate(PropertyExpr("var"), RegexExpr("""a\.*.*b""")))
    }

    "support like predicate" in {
      QueryParser.parse("""var like 'a%b' """) must beSuccessfulTry[BooleanExpr].withValue(
        LikePredicate(PropertyExpr("var"), LikeExpr("""a%b""")))
    }

    "support ilike predicate" in {
      QueryParser.parse("""var ilike 'a%b' """) must beSuccessfulTry[BooleanExpr].withValue(
        ILikePredicate(PropertyExpr("var"), LikeExpr("""a%b""")))
    }

    "support the is null predicate" in {
      QueryParser.parse(""" var is null """) must beSuccessfulTry[BooleanExpr].withValue(
        NullTestPredicate(PropertyExpr("var"), isNull = true))
    }

    "support the is not null predicate" in {
      QueryParser.parse(""" var is NOT NULL """) must beSuccessfulTry[BooleanExpr].withValue(
        NullTestPredicate(PropertyExpr("var"), isNull = false))
    }

    "support the intersects predicate with bbox" in {
      QueryParser.parse(""" intersects bbox """) must beSuccessfulTry[BooleanExpr].withValue(
        IntersectsPredicate(None))
    }

    "support the intersects predicate with WKT Expression" in {
      QueryParser.parse(""" intersects 'SRID=31370;POINT(1 1)' """) must beSuccessfulTry[BooleanExpr].withValue(
        IntersectsPredicate(Some("SRID=31370;POINT(1 1)")))
    }

    "support the json contains predicate " in {
      QueryParser.parse(""" var @>  '["a",2]' """) must beSuccessfulTry[BooleanExpr].withValue(
        JsonContainsPredicate(PropertyExpr("var"), LiteralString("""["a",2]"""))) and (
          QueryParser.parse(""" var @>  '[''a'',2]' """) must beSuccessfulTry[BooleanExpr].withValue(
            JsonContainsPredicate(PropertyExpr("var"), LiteralString("""['a',2]""")))) and (
              QueryParser.parse(""" var @> '{"a": "b"}' """) must beSuccessfulTry[BooleanExpr].withValue(
                JsonContainsPredicate(PropertyExpr("var"), LiteralString("""{"a": "b"}"""))))
    }

    "support the to_date function for strings" in {
      QueryParser.parse(""" var = to_date( '2019-04-30', 'YYYY-MM-DD')""") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(
          PropertyExpr("var"),
          EQ,
          ToDate(LiteralString("2019-04-30"), LiteralString("YYYY-MM-DD"))))
    }

    "support the to_date function on properties" in {
      QueryParser.parse(""" var = to_date( a.b.c, 'YYYY-MM-DD')""") must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(
          PropertyExpr("var"),
          EQ,
          ToDate(PropertyExpr("a.b.c"), LiteralString("YYYY-MM-DD"))))
    }

    "supports comparison on dates" in {
      QueryParser.parse(""" to_date( a.b.c, 'YYYY-MM-DD') = to_date('2019-04-30', 'YYYY-MM-DD') """) must beSuccessfulTry[BooleanExpr].withValue(
        ComparisonPredicate(
          ToDate(PropertyExpr("a.b.c"), LiteralString("YYYY-MM-DD")),
          EQ,
          ToDate(LiteralString("2019-04-30"), LiteralString("YYYY-MM-DD"))))
    }

    "support the between .. and ..  expression" in {
      QueryParser.parse(""" to_date( a.b.c, 'YYYY-MM-DD') between '2011-01-01' and '2012-01-01' """) must beSuccessfulTry[BooleanExpr].withValue(
        BetweenAndPredicate(
          ToDate(PropertyExpr("a.b.c"), LiteralString("YYYY-MM-DD")),
          LiteralString("2011-01-01"),
          LiteralString("2012-01-01")))
    }

  }

}
