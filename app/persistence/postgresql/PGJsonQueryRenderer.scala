package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 23/01/15.
 */
object PGJsonQueryRenderer extends BaseQueryRenderer {

  protected def renderPropertyExpr(lhs: PropertyExpr, rhs: Expr): String = {
    val variadicPath: String = path2VariadicList(lhs)
    s"json_extract_path_text(json, $variadicPath)::${cast(rhs)}"
  }

  protected def renderPropertyExprwithoutCast(lhs: PropertyExpr): String = {
    val variadicPath: String = path2VariadicList(lhs)
    s"json_extract_path_text(json, $variadicPath)"
  }

  def cast(exp: Expr): String = exp match {
    case LiteralBoolean(_) => "bool"
    case LiteralNumber(_) => "decimal"
    case LiteralString(_) => "text"
    case ValueListExpr(values) => cast(values.head)
    case RegexExpr(_) => "text"
    case LikeExpr(_) => "text"
    case _ => "text"
  }

  def path2VariadicList(propertyExpr: PropertyExpr): String = "'" + propertyExpr.path.replaceAll("\\.", "','") + "'"

  override def renderBooleanAnd(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  ): String = s" ( ${render(lhs)} ) AND ( ${render(rhs)} )"

  override def renderBooleanOr(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  ): String = s" ( ${render(lhs)} ) OR ( ${render(rhs)} )"

  override def renderBooleanNot(inner: BooleanExpr): String = s" NOT ( ${render(inner)} ) "

  override def renderComparisonPredicate(
    lhs: PropertyExpr,
    op: ComparisonOperator,
    rhs: ValueExpr
  ): String = s" ${renderPropertyExpr(lhs, rhs)} ${sym(op)} ( ${renderValue(rhs)} )"

  override def renderInPredicate(
    lhs: PropertyExpr,
    rhs: ValueListExpr
  ): String = s" ${renderPropertyExpr(lhs, rhs)} in ${renderValueList(rhs)}"

  override def renderRegexPredicate(
    lhs: PropertyExpr,
    rhs: RegexExpr
  ): String = s" ${renderPropertyExpr(lhs, rhs)} ~ '${rhs.pattern}'"

  override def renderLikePredicate(
    lhs: PropertyExpr,
    rhs: LikeExpr
  ): String = s" ${renderPropertyExpr(lhs, rhs)} ilike '${rhs.pattern}'"

  override def renderNullTestPredicate(
    lhs: PropertyExpr,
    is: Boolean
  ): String = s" ${renderPropertyExprwithoutCast(lhs)} ${if (is) "is" else "is not"} null"
}
