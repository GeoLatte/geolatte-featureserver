package querylang

import org.specs2.mutable.Specification

/**
 * Created by Karel Maesen, Geovise BVBA on 15/01/15.
 */
class QueryParserSpec extends Specification {

  "A QueryParser " should {


    "parse equality expressions correctly " in {
      querylang.QueryParser.parse("var =  '123' ") must beSuccessfulTry.withValue(
        ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123")))
    }

    "parse greater or equals expressions correctly " in {
      querylang.QueryParser.parse("var >=  '123' ") must beSuccessfulTry.withValue(
        ComparisonPredicate(PropertyExpr("var"), GTE, LiteralString("123")))
    }

    "handle parentesis properly" in {
      querylang.QueryParser.parse("( var =  '123' )") must beSuccessfulTry.withValue(
        ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123")))
    }

    "Interpret literal numbers  properly" in {

      (
        querylang.QueryParser.parse(" var =  -123 ") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(-123))))
        ) and (
        querylang.QueryParser.parse("var =  123.54") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54))))
        ) and (
        querylang.QueryParser.parse(" var =  123.54E3 ") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E3))))
        ) and (
        querylang.QueryParser.parse(" var =  123.54E-3") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E-3))))
        )

    }

    "Interpret literal strings  properly" in {

      (
        querylang.QueryParser.parse(" var =  'abc.d' ") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("abc.d")))
        ) and (
        querylang.QueryParser.parse("var =  '!abc._-$%^&*()@#$%%^_+00==\"'") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("!abc._-$%^&*()@#$%%^_+00==\"")))
        ) and (
        querylang.QueryParser.parse(" var =  123.54E3 ") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E3))))
        ) and (
        querylang.QueryParser.parse(" var =  123.54E-3") must beSuccessfulTry.withValue(
          ComparisonPredicate(PropertyExpr("var"), EQ, LiteralNumber(BigDecimal(123.54E-3))))
        )

    }


    "handle negations properly" in {

      (
        querylang.QueryParser.parse(" not (var =  '123')") must beSuccessfulTry.withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123"))))
        ) and (
        querylang.QueryParser.parse(" not var =  '123'") must beSuccessfulTry.withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralString("123"))))
        )

    }


    "handle AND combinator properly" in {

      (
        querylang.QueryParser.parse("(vara > 12) and not (varb =  '123')") must beSuccessfulTry.withValue(BooleanAnd(ComparisonPredicate(
          PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))), BooleanNot(
          ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123")))))
        ) and (
        querylang.QueryParser.parse("vara > 12 and varb =  '123'") must beSuccessfulTry.withValue( BooleanAnd(ComparisonPredicate(
          PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))), ComparisonPredicate(
          PropertyExpr("varb"), EQ, LiteralString("123"))))
        ) and (
        querylang.QueryParser.parse("vara > 12 AND varb =  '123'") must beSuccessfulTry.withValue( BooleanAnd(ComparisonPredicate(
          PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))), ComparisonPredicate(
          PropertyExpr("varb"), EQ, LiteralString("123"))))
        )

    }

    "handle consecutive AND combinator without needing nesting parenthesis" in {
      querylang.QueryParser.parse("vara > 12 and varb =  '123' and varc = 'bla'") must beSuccessfulTry.withValue(
        BooleanAnd(
          BooleanAnd(
            ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
            ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))
          ),
          ComparisonPredicate(PropertyExpr("varc"), EQ, LiteralString("bla"))
        )
      )
    }

    "handle OR combinator properly" in {
      (
        querylang.QueryParser.parse("(vara > 12) or not (varb =  '123')") must beSuccessfulTry.withValue(
          BooleanOr(
          ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
          BooleanNot(ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123")))))
        ) and (
        querylang.QueryParser.parse("vara > 12 or varb =  '123'") must beSuccessfulTry.withValue(
          BooleanOr(
          ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
          ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))))
        ) and (
        querylang.QueryParser.parse("vara > 12 or ( varb =  '123' and varc = 'abc' ) ") must beSuccessfulTry.withValue(
          BooleanOr(
          ComparisonPredicate(PropertyExpr("vara"),GT,LiteralNumber(BigDecimal(12))),
          BooleanAnd(
            ComparisonPredicate(PropertyExpr("varb"),EQ,LiteralString("123")),
            ComparisonPredicate(PropertyExpr("varc"),EQ,LiteralString("abc")))))
        )
    }

    "handle consecutive OR combinator without needing nesting parenthesis" in {
      querylang.QueryParser.parse("vara > 12 or varb =  '123' or varc = 'bla'") must beSuccessfulTry.withValue(
        BooleanOr(
          BooleanOr(
            ComparisonPredicate(PropertyExpr("vara"), GT, LiteralNumber(BigDecimal(12))),
            ComparisonPredicate(PropertyExpr("varb"), EQ, LiteralString("123"))
          ),
          ComparisonPredicate(PropertyExpr("varc"), EQ, LiteralString("bla"))
        )
      )
    }

    "handle boolean literal values in expression" in {
      (
        querylang.QueryParser.parse(" not (var =  true)") must beSuccessfulTry.withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralBoolean(true))))
        ) and (
        querylang.QueryParser.parse(" not var =  false") must beSuccessfulTry.withValue(
          BooleanNot(ComparisonPredicate(PropertyExpr("var"), EQ, LiteralBoolean(false))))
        )
    }

    "treat a boolean literal as a valid expression" in {
      (
        querylang.QueryParser.parse("true") must beSuccessfulTry.withValue(
          LiteralBoolean(true))
        ) and (
        querylang.QueryParser.parse(" not (false)") must beSuccessfulTry.withValue(
          BooleanNot(LiteralBoolean(false)))
        )
    }

    "supports the in operator" in {
      (querylang.QueryParser.parse("var in ('a', 'b', 'c')") must beSuccessfulTry.withValue(
        InPredicate(PropertyExpr("var"), ValueListExpr(List(LiteralString("c"), LiteralString("b"), LiteralString("a"))))
      )) and ( querylang.QueryParser.parse("var in  (  1,2, 3 )") must beSuccessfulTry.withValue(
        InPredicate(PropertyExpr("var"), ValueListExpr(List(LiteralNumber(3), LiteralNumber(2), LiteralNumber(1))))
      ))
    }

  }

}
