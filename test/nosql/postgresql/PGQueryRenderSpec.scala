package nosql.postgresql


import org.specs2.mutable.Specification
import querylang.{QueryParser, BooleanExpr}

import scala.util.Try

/**
* Created by Karel Maesen, Geovise BVBA on 23/01/15.
*/
class PGQueryRenderSpec extends Specification {

  "The PGQueryRenderer " should {

    val renderer = PGQueryRenderer

    "properly render boolean expressions containing equality expresssions " in {
      val expr: BooleanExpr = QueryParser.parse("ab.cd = 12").get
      compressWS(renderer.render(expr)) ===   "json_extract_path_text(json, 'ab','cd')::decimal = ( 12 )"
    }

    "properly render comparison expressions contain boolean literal" in {
      val expr = QueryParser.parse("ab.cd = TRUE").get
      compressWS(renderer.render(expr))  === "json_extract_path_text(json, 'ab','cd')::bool = ( true )"
    }

    "properly render simple equality expression " in {
      val expr = QueryParser.parse("properties.foo = 'bar1'").get
      compressWS(renderer.render(expr)) === "json_extract_path_text(json, 'properties','foo')::text = ( 'bar1' )"
    }


    "properly render boolean expressions containing comparison expresssions other than equality " in {

      val expr1 = QueryParser.parse("ab.cd > 12").get
      val expr2 = QueryParser.parse("ab.cd >= 12").get
      val expr3 = QueryParser.parse("ab.cd != 'abc'").get

      (
        compressWS(renderer.render(expr1)) === "json_extract_path_text(json, 'ab','cd')::decimal > ( 12 )"
        ) and (
        compressWS(renderer.render(expr2)) === "json_extract_path_text(json, 'ab','cd')::decimal >= ( 12 )"
        ) and (
        compressWS(renderer.render(expr3)) ===  "json_extract_path_text(json, 'ab','cd')::text != ( 'abc' )"
        )
    }

    "properly render negated expressions " in {
      val expr1 = QueryParser.parse("not ab.cd = 12").get
      compressWS(renderer.render(expr1)) === "NOT ( json_extract_path_text(json, 'ab','cd')::decimal = ( 12 ) )"
    }

    "properly render AND expressions " in {
      val expr1 = QueryParser.parse(" (ab = 12) and (cd > 'c') ").get
      compressWS(renderer.render(expr1)) ===
        "( json_extract_path_text(json, 'ab')::decimal = ( 12 ) ) AND ( json_extract_path_text(json, 'cd')::text > ( 'c' ) )"
    }

    "properly render OR expressions " in {
      val expr1 = QueryParser.parse(" (ab = 12) or (cd > 'c') ").get
      compressWS(renderer.render(expr1)) ===
        "( json_extract_path_text(json, 'ab')::decimal = ( 12 ) ) OR ( json_extract_path_text(json, 'cd')::text > ( 'c' ) )"
    }

    "properly render a boolean literal as a boolean expression" in {
      val expr = QueryParser.parse("TRUE").get
      renderer.render(expr)  === " true "
    }

  }

  private def compressWS(str : String) = str.replaceAll(" +", " ").trim

}
