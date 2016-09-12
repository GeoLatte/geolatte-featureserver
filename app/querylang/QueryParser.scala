package querylang

import org.parboiled2._
import scala.util.{ Try, Success, Failure }

/**
 * A parser for simple Query expression
 *
 * Created by Karel Maesen, Geovise BVBA on 15/01/15.
 */

sealed trait Expr

sealed trait BooleanExpr
case class BooleanOr(lhs: BooleanExpr, rhs: BooleanExpr) extends BooleanExpr
case class BooleanAnd(lhs: BooleanExpr, rhs: BooleanExpr) extends BooleanExpr
case class BooleanNot(expr: BooleanExpr) extends BooleanExpr

sealed trait Predicate extends BooleanExpr
case class ComparisonPredicate(lhs: PropertyExpr, op: ComparisonOperator, rhs: ValueExpr) extends Predicate
case class InPredicate(lhs: PropertyExpr, rhs: ValueListExpr) extends Predicate
case class RegexPredicate(lhs: PropertyExpr, rhs: RegexExpr) extends Predicate
case class LikePredicate(lhs: PropertyExpr, rhs: LikeExpr) extends Predicate

sealed trait ValueExpr extends Expr
case class LiteralString(value: String) extends ValueExpr
case class LiteralNumber(value: BigDecimal) extends ValueExpr
case class LiteralBoolean(value: Boolean) extends ValueExpr with BooleanExpr

case class ValueListExpr(values: List[ValueExpr]) extends Expr

case class RegexExpr(pattern: String) extends Expr
case class LikeExpr(pattern: String) extends Expr

case class PropertyExpr(path: String) extends Expr

sealed trait ComparisonOperator
case object EQ extends ComparisonOperator
case object NEQ extends ComparisonOperator
case object LT extends ComparisonOperator
case object GT extends ComparisonOperator
case object LTE extends ComparisonOperator
case object GTE extends ComparisonOperator

class QueryParserException(message: String = null) extends RuntimeException(message)

class QueryParser(val input: ParserInput) extends Parser
    with StringBuilding {

  def InputLine = rule { BooleanExpression ~ EOI }

  def BooleanExpression: Rule1[BooleanExpr] = rule { BooleanTerm ~ WS ~ zeroOrMore(ignoreCase("or") ~ WS ~ BooleanTerm ~> BooleanOr) }

  def BooleanTerm: Rule1[BooleanExpr] = rule { BooleanFactor ~ WS ~ zeroOrMore(ignoreCase("and") ~ WS ~ BooleanFactor ~> BooleanAnd) }

  def BooleanFactor = rule { WS ~ ignoreCase("not") ~ WS ~ BooleanPrim ~> BooleanNot | BooleanPrim }

  def BooleanPrim = rule { WS ~ ch('(') ~ WS ~ BooleanExpression ~ WS ~ ch(')') ~ WS | Predicate }

  def Predicate = rule { ComparisonPred | InPred | LikePred | RegexPred | LiteralBool }

  def LikePred = rule { (WS ~ Property ~ WS ~ ignoreCase("like") ~ WS ~ Like) ~> LikePredicate }

  def RegexPred = rule { (WS ~ Property ~ WS ~ "~" ~ WS ~ Regex) ~> RegexPredicate }

  def InPred = rule { (WS ~ Property ~ WS ~ ignoreCase("in") ~ WS ~ ExpressionList ~ WS) ~> InPredicate }

  def ComparisonPred = rule { (WS ~ Property ~ WS ~ ComparisonOp ~ Expression ~ WS) ~> ComparisonPredicate }

  def ComparisonOp = rule { ">=" ~ push(GTE) | "<=" ~ push(LTE) | "=" ~ push(EQ) | "!=" ~ push(NEQ) | "<" ~ push(LT) | ">" ~ push(GT) }

  val toValList: ValueExpr => ValueListExpr = v => ValueListExpr(List(v))
  val combineVals: (ValueListExpr, ValueExpr) => ValueListExpr = (list, ve) => ValueListExpr(ve :: list.values)
  def ExpressionList = rule { WS ~ "(" ~ WS ~ (Expression ~> toValList) ~ WS ~ zeroOrMore("," ~ WS ~ (Expression ~> combineVals) ~ WS) ~ WS ~ ")" }

  def Expression = rule { LiteralBool | LiteralStr | LiteralNum }

  private val toNum: String => LiteralNumber = (s: String) => LiteralNumber(BigDecimal(s))

  def LiteralBool = rule { (ignoreCase("true") ~ push(LiteralBoolean(true))) | (ignoreCase("false") ~ push(LiteralBoolean(false))) }

  def LiteralNum = rule { capture(Number) ~> toNum }

  val printableChar = (CharPredicate.Printable -- "'")

  def LiteralStr = rule { ch(''') ~ clearSB() ~ zeroOrMore((printableChar | "\'\'") ~ appendSB()) ~ ch(''') ~ push(LiteralString(sb.toString)) }

  def Regex = rule { ch('/') ~ clearSB() ~ zeroOrMore((noneOf("/") ~ appendSB())) ~ ch('/') ~ push(RegexExpr(sb.toString)) }

  def Like = rule { ch(''') ~ clearSB() ~ zeroOrMore((printableChar | "\'\'") ~ appendSB()) ~ ch(''') ~ push(LikeExpr(sb.toString)) }

  def Property = rule { capture(NameString) ~> PropertyExpr ~ WS }

  //basic tokens
  def NameString = rule { !(ch(''') | ch('"')) ~ FirstNameChar ~ zeroOrMore(nonFirstNameChar) }

  def FirstNameChar = rule { CharPredicate.Alpha | ch('_') }

  def nonFirstNameChar = rule { CharPredicate.AlphaNum | ch('_') | ch('-') | ch('.') }

  def Number = rule { optional('-') ~ WS ~ Digits ~ optional('.' ~ optional(Digits)) ~ optional('E' ~ optional('-') ~ Digits) }

  def Digits = rule { oneOrMore(CharPredicate.Digit) }

  def WS = rule { zeroOrMore(" " | "\n" | "\t") }

  implicit def wspStr(s: String): Rule0 = rule {
    str(s) ~ zeroOrMore(' ')
  }

}

object QueryParser {
  def parse(s: String): Try[BooleanExpr] = {
    val parser = new QueryParser(s)
    //parse input, and in case of ParseErrors format a nice message
    parser.InputLine.run() match {
      case s @ Success(_) => s
      case Failure(pe: ParseError) => Failure(new QueryParserException(parser.formatError(pe, showExpected = true, showPosition = true)))
      case f @ Failure(_) => f
    }
  }

}
