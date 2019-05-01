package persistence.querylang

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
case class ComparisonPredicate(lhs: AtomicExpr, op: ComparisonOperator, rhs: AtomicExpr) extends Predicate
case class InPredicate(lhs: AtomicExpr, rhs: ValueListExpr) extends Predicate
case class RegexPredicate(lhs: AtomicExpr, rhs: RegexExpr) extends Predicate
case class LikePredicate(lhs: AtomicExpr, rhs: LikeExpr) extends Predicate
case class NullTestPredicate(lhs: AtomicExpr, isNull: Boolean) extends Predicate
case class IntersectsPredicate(wkt: Option[String]) extends Predicate {
  def intersectsWithBbox: Boolean = wkt.isDefined
}
case class JsonContainsPredicate(lhs: PropertyExpr, rhs: LiteralString) extends Predicate

sealed trait AtomicExpr extends Expr

case class LiteralString(value: String) extends AtomicExpr
case class LiteralNumber(value: BigDecimal) extends AtomicExpr
case class LiteralBoolean(value: Boolean) extends AtomicExpr with BooleanExpr
case class PropertyExpr(path: String) extends AtomicExpr
case class ToDate(date: AtomicExpr, fmt: AtomicExpr) extends AtomicExpr

case class ValueListExpr(values: List[AtomicExpr]) extends Expr

case class RegexExpr(pattern: String) extends Expr
case class LikeExpr(pattern: String) extends Expr

sealed trait ComparisonOperator
case object EQ extends ComparisonOperator
case object NEQ extends ComparisonOperator
case object LT extends ComparisonOperator
case object GT extends ComparisonOperator
case object LTE extends ComparisonOperator
case object GTE extends ComparisonOperator

sealed trait Arg
case class PropertyArg(propery: PropertyExpr) extends Arg
case class ValueArg(value: AtomicExpr) extends Arg

class QueryParserException(message: String = null) extends RuntimeException(message)

class QueryParser(val input: ParserInput) extends Parser
    with StringBuilding {

  def InputLine = rule { BooleanExpression ~ EOI }

  def BooleanExpression: Rule1[BooleanExpr] = rule { BooleanTerm ~ WS ~ zeroOrMore(ignoreCase("or") ~ WS ~ BooleanTerm ~> BooleanOr) }

  def BooleanTerm: Rule1[BooleanExpr] = rule { BooleanFactor ~ WS ~ zeroOrMore(ignoreCase("and") ~ WS ~ BooleanFactor ~> BooleanAnd) }

  def BooleanFactor = rule { WS ~ ignoreCase("not") ~ WS ~ BooleanPrim ~> BooleanNot | BooleanPrim }

  def BooleanPrim = rule { WS ~ ch('(') ~ WS ~ BooleanExpression ~ WS ~ ch(')') ~ WS | Predicate }

  def Predicate = rule { spatialRelPred | ComparisonPred | InPred | LikePred | RegexPred | LiteralBool | isNullPred | JsonContainsPred }

  def spatialRelPred = rule { intersectsTest }

  def intersectsTest = rule { (WS ~ ignoreCase("intersects") ~ WS ~ GeomLiteral) ~> IntersectsPredicate }

  def GeomLiteral = rule { (ignoreCase("bbox") ~ push(None)) | LiteralStr ~> (s => Some(s.value)) }

  def isNullPred = rule { WS ~ AtomicExpression ~ WS ~ MayBe ~ WS ~ ignoreCase("null") ~> NullTestPredicate }

  def MayBe: Rule1[Boolean] = rule { ignoreCase("is") ~ push(true) ~ WS ~ optional(ignoreCase("not") ~> ((_: Boolean) => false)) }

  def LikePred = rule { (WS ~ AtomicExpression ~ WS ~ ignoreCase("like") ~ WS ~ Like) ~> LikePredicate }

  def RegexPred = rule { (WS ~ AtomicExpression ~ WS ~ "~" ~ WS ~ Regex) ~> RegexPredicate }

  def InPred = rule { (WS ~ AtomicExpression ~ WS ~ ignoreCase("in") ~ WS ~ ExpressionList ~ WS) ~> InPredicate }

  def ComparisonPred = rule { (WS ~ AtomicExpression ~ WS ~ ComparisonOp ~ AtomicExpression ~ WS) ~> ComparisonPredicate }

  def ComparisonOp = rule { ">=" ~ push(GTE) | "<=" ~ push(LTE) | "=" ~ push(EQ) | "!=" ~ push(NEQ) | "<" ~ push(LT) | ">" ~ push(GT) }

  def JsonContainsPred = rule { (WS ~ Property ~ WS ~ ignoreCase("@>") ~ WS ~ LiteralStr ~ WS) ~> JsonContainsPredicate }

  val toValList: AtomicExpr => ValueListExpr = v => ValueListExpr(List(v))
  val combineVals: (ValueListExpr, AtomicExpr) => ValueListExpr = (list, ve) => ValueListExpr(ve :: list.values)
  def ExpressionList = rule { WS ~ "(" ~ WS ~ (AtomicExpression ~> toValList) ~ WS ~ zeroOrMore("," ~ WS ~ (AtomicExpression ~> combineVals) ~ WS) ~ WS ~ ")" }

  def AtomicExpression = rule { FunctionApp | LiteralBool | LiteralStr | LiteralNum | Property }

  def FunctionApp = rule { ToDateApp }

  def ToDateApp = rule { (WS ~ ignoreCase("to_date") ~ WS ~ "(" ~ (LiteralStr | Property) ~ WS ~ "," ~ WS ~ LiteralStr ~ WS ~ ")" ~ WS) ~> ToDate }

  private val toNum: String => LiteralNumber = (s: String) => LiteralNumber(BigDecimal(s))

  def LiteralBool = rule { (ignoreCase("true") ~ push(LiteralBoolean(true))) | (ignoreCase("false") ~ push(LiteralBoolean(false))) }

  def LiteralNum = rule { capture(Number) ~> toNum }

  val printableChar = (CharPredicate.Printable -- "'")

  //a Literal String is quoted using single quotes. To use singe quotes in the string, simply repeat the quote twice (with no whitespace).
  def LiteralStr = rule { ch(''') ~ clearSB() ~ zeroOrMore((printableChar | "''") ~ appendSB()) ~ ch(''') ~ push(LiteralString(sb.toString)) }

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
      case Failure(pe: ParseError) => Failure(new QueryParserException(parser.formatError(pe, new ErrorFormatter(showExpected = true, showPosition = true))))
      case f @ Failure(_) => f
    }
  }

}
