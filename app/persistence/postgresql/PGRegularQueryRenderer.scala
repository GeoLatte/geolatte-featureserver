package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
object PGRegularQueryRenderer extends BaseQueryRenderer {

  object PER extends PropertyExprRenderer {
    override def render(lhs: PropertyExpr): String = if (lhs.path.trim.startsWith("properties.")) lhs.path.trim.substring(11)
    else lhs.path
  }

  override def defaultPropertyExprRenderer: PGRegularQueryRenderer.PropertyExprRenderer = PER

  protected def renderPropertyExpr(lhs: PropertyExpr): String =
    if (lhs.path.trim.startsWith("properties.")) lhs.path.trim.substring(11)
    else lhs.path

  def renderAtomic(lhs: AtomicExpr): String =
    super.renderAtomic(PER)(lhs)

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
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} ${sym(op)} ( ${renderAtomic(rhs)} )"

  override def renderInPredicate(
    lhs: AtomicExpr,
    rhs: ValueListExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} in ${renderValueList(rhs, PER)}"

  override def renderRegexPredicate(
    lhs: AtomicExpr,
    rhs: RegexExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} ~ '${rhs.pattern}'"

  override def renderLikePredicate(
    lhs: AtomicExpr,
    rhs: LikeExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} ilike '${rhs.pattern}'"

  override def renderNullTestPredicate(
    lhs: AtomicExpr,
    is: Boolean
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} ${if (is) "is" else "is not"} null"

}
