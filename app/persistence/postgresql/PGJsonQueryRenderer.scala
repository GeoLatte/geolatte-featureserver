package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 23/01/15.
 */
object PGJsonQueryRenderer extends BaseQueryRenderer {

  object PropertyPathAsJson extends PropertyExprRenderer {
    override def render(expr: PropertyExpr): String = {
      val variadicPath = path2VariadicList(expr)
      s"json_extract_path(json, $variadicPath)"
    }
  }

  object PropertyPathAsJsonText extends PropertyExprRenderer {

    override def render(expr: PropertyExpr): String = {
      val variadicPath = path2VariadicList(expr)
      s"json_extract_path_text(json, $variadicPath)"
    }
  }

  override def defaultPropertyExprRenderer: PGJsonQueryRenderer.PropertyExprRenderer = PropertyPathAsJson

  def cast(exp: Expr): String = exp match {
    case LiteralBoolean(_) => "bool"
    case LiteralNumber(_) => "decimal"
    case LiteralString(_) => "text"
    case ValueListExpr(values) => cast(values.head)
    case RegexExpr(_) => "text"
    case LikeExpr(_) => "text"
    case _ => "text"
  }

  override def renderBooleanAnd(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  )(implicit ctxt: RenderContext): String = s" ( ${render(lhs)} ) AND ( ${render(rhs)} )"

  override def renderBooleanOr(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  )(implicit ctxt: RenderContext): String = s" ( ${render(lhs)} ) OR ( ${render(rhs)} )"

  override def renderBooleanNot(inner: BooleanExpr)(implicit ctxt: RenderContext): String = s" NOT ( ${render(inner)} ) "

  override def renderComparisonPredicate(
    lhs: AtomicExpr,
    op: ComparisonOperator,
    rhs: AtomicExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} ${sym(op)} ( ${renderAtomic(PropertyPathAsJson)(rhs)} )"

  override def renderInPredicate(
    lhs: AtomicExpr,
    rhs: ValueListExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} in ${renderValueList(rhs, PropertyPathAsJson)}"

  override def renderRegexPredicate(
    lhs: AtomicExpr,
    rhs: RegexExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} ~ '${rhs.pattern}'"

  override def renderLikePredicate(
    lhs: AtomicExpr,
    rhs: LikeExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} ilike '${rhs.pattern}'"

  override def renderNullTestPredicate(
    lhs: AtomicExpr,
    is: Boolean
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(PropertyPathAsJsonText)(lhs)} ${
    if (is) {
      "is"
    } else {
      "is not"
    }
  } null"

  override def renderToDate(
    date: AtomicExpr,
    fmt: AtomicExpr
  ): String = s" to_date(${renderAtomic(PropertyPathAsJsonText)(date)}, ${renderAtomic(PropertyPathAsJsonText)(fmt)}) "

  private def renderPropertyExprwithoutCast(lhs: PropertyExpr): String = {
    val variadicPath: String = path2VariadicList(lhs)
    s"json_extract_path_text(json, $variadicPath)"
  }

  private def path2VariadicList(propertyExpr: PropertyExpr): String =
    "'" + propertyExpr.path.replaceAll("\\.", "','") + "'"

  private def renderAtomicCasting(lhs: AtomicExpr, rhs: Expr): String = lhs match {
    case p @ PropertyExpr(_) => s"${PropertyPathAsJsonText.render(p)}::${cast(rhs)}"
    case _ => renderAtomic(PropertyPathAsJson)(lhs)
  }

}
