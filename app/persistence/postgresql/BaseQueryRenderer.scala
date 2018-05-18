package persistence.postgresql

import persistence.postgresql.PGRegularQueryRenderer.{ renderPropertyExpr, renderValue, renderValueList, sym }
import persistence.querylang.{ GTE, LTE, _ }

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
trait BaseQueryRenderer extends QueryRenderer[String] {

  def renderBooleanAnd(lhs: BooleanExpr, rhs: BooleanExpr): String

  def renderBooleanOr(lhs: BooleanExpr, rhs: BooleanExpr): String

  def renderBooleanNot(inner: BooleanExpr): String

  def renderLiteralBoolean(b: Boolean): String = if (b) " true " else " false "

  def renderComparisonPredicate(lhs: PropertyExpr, op: ComparisonOperator, rhs: ValueExpr): String

  def renderInPredicate(lhs: PropertyExpr, rhs: ValueListExpr): String

  def renderRegexPredicate(lhs: PropertyExpr, rhs: RegexExpr): String

  def renderLikePredicate(lhs: PropertyExpr, rhs: LikeExpr): String

  def renderNullTestPredicate(lhs: PropertyExpr, is: Boolean): String

  def render(expr: BooleanExpr): String = expr match {
    case BooleanAnd(lhs, rhs) => renderBooleanAnd(lhs, rhs)
    case BooleanOr(lhs, rhs) => renderBooleanOr(lhs, rhs)
    case BooleanNot(inner) => renderBooleanNot(inner)
    case LiteralBoolean(b) => renderLiteralBoolean(b)
    case ComparisonPredicate(lhs, op, rhs) => renderComparisonPredicate(lhs, op, rhs)
    case InPredicate(lhs, rhs) => renderInPredicate(lhs, rhs)
    case RegexPredicate(lhs, rhs) => renderRegexPredicate(lhs, rhs)
    case LikePredicate(lhs, rhs) => renderLikePredicate(lhs, rhs)
    case NullTestPredicate(lhs, is) => renderNullTestPredicate(lhs, is)
  }

  def renderValue(expr: ValueExpr): String = expr match {
    case LiteralBoolean(b) => if (b) " true " else " false "
    case LiteralNumber(n) => s" ${n.toString} "
    case LiteralString(s) => s" '$s' "
  }

  def renderValueList(expr: ValueListExpr): String =
    s"(${expr.values.map(renderValue).map(_.trim).mkString(",")})"

  def sym(op: ComparisonOperator): String = op match {
    case EQ => " = "
    case NEQ => " != "
    case LT => " < "
    case GT => " > "
    case LTE => " <= "
    case GTE => " >= "
  }

}
