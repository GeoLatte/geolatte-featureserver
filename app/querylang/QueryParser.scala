package querylang


import org.parboiled2._
import shapeless.{HList, HNil}

import scala.util.{Try, Success, Failure}

/**
* A parser for simple Query expression
*
* Created by Karel Maesen, Geovise BVBA on 15/01/15.
*/


sealed trait Expr

sealed trait BooleanExpr
case class BooleanOr(rhs: BooleanExpr, lhs: BooleanExpr) extends BooleanExpr
case class BooleanAnd(rhs: BooleanExpr, lhs: BooleanExpr) extends BooleanExpr
case class BooleanNot(expr: BooleanExpr) extends BooleanExpr

sealed trait Predicate extends BooleanExpr
case class ComparisonPredicate(rhs: ValueExpr, op: ComparisonOperator, lhs: ValueExpr) extends Predicate

sealed trait ValueExpr extends Expr
case class LiteralString(value: String) extends ValueExpr
case class LiteralNumber(value: BigDecimal) extends ValueExpr
case class PropertyName(value: String) extends ValueExpr

sealed trait ComparisonOperator
case object EQ extends ComparisonOperator
case object NEQ extends ComparisonOperator
case object LT extends ComparisonOperator
case object GT extends ComparisonOperator
case object LTE extends ComparisonOperator
case object GTE extends ComparisonOperator


class QueryParser (val input: ParserInput ) extends Parser {


  def InputLine = rule { BooleanExpression ~ EOI }

  def BooleanExpression :  Rule1[BooleanExpr] = rule { ( ( BooleanTerm ~ WS ~ ignoreCase("or") ~ WS ~ BooleanTerm ) ~> BooleanOr ) | BooleanTerm }

  def BooleanTerm = rule { ( ( BooleanFactor ~ WS ~ ignoreCase("and") ~ WS ~ BooleanFactor ) ~> BooleanAnd ) | BooleanFactor }

  def BooleanFactor = rule { WS ~ ignoreCase("not") ~ WS ~ BooleanPrim ~> BooleanNot | BooleanPrim}

  def BooleanPrim = rule { WS ~ ch('(') ~ WS ~ BooleanExpression ~ WS ~ ch(')') ~ WS | Predicate  }

  def Predicate = rule { Comparison }

  def Comparison = rule { (WS ~ Expression ~ WS ~ ComparisonOp ~ Expression ~ WS )  ~> ComparisonPredicate }

  def ComparisonOp =  rule { ">=" ~ push(GTE) | "<=" ~ push(LTE) | "=" ~ push(EQ) | "!=" ~ push(NEQ) | "<" ~ push(LT) | ">" ~ push(GT)  }

  def Expression = rule { Property | LiteralStr | LiteralNum }

  private val toNum : String => LiteralNumber =  (s: String) =>  LiteralNumber( BigDecimal(s) )

  def LiteralNum = rule {  capture(Number)  ~> toNum  }

  def LiteralStr = rule {  ch(''') ~ (capture(zeroOrMore(CharPredicate.AlphaNum)) ~> LiteralString ) ~ ch(''') }

  def Property = rule { capture(NameString)  ~> PropertyName ~ WS}


  //basic tokens
  def NameString = rule { !( ch(''') | ch('"') ) ~ FirstNameChar ~ zeroOrMore(nonFirstNameChar) }

  def FirstNameChar = rule { CharPredicate.Alpha | ch('_') }

  def nonFirstNameChar = rule { CharPredicate.AlphaNum  |  ch('_') | ch('-') | ch('.') }

  def Number = rule { optional('-') ~ WS ~ Digits ~ optional('.' ~ optional(Digits)) ~ optional('E' ~ optional('-') ~ Digits) }

  def Digits = rule { oneOrMore(CharPredicate.Digit)}

  def WS = rule { zeroOrMore(" " |  "\n" | "\t") }

  implicit def wspStr(s: String): Rule0 = rule {
    str(s) ~ zeroOrMore(' ')
  }


}


object QueryParser {
  def parse(s: String) : Try[BooleanExpr] = {
    val parser = new QueryParser(s)
    parser.InputLine.run()
  }
}
