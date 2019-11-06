package persistence.postgresql

import persistence.querylang.{ GTE, LTE, _ }

case class RenderContext(geometryColumn: String, bbox: Option[String] = None)

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
trait BaseQueryRenderer extends QueryRenderer[String, RenderContext] {

  def renderPropertyExpr(exp: PropertyExpr): String

  def renderAtomic(expr: AtomicExpr): String = expr match {
    case ToDate(date, fmt)   => renderToDate(date, fmt)
    case LiteralBoolean(b)   => if (b) " true " else " false "
    case LiteralNumber(n)    => s" ${n.toString} "
    case LiteralString(s)    => s" '$s' "
    case p @ PropertyExpr(_) => renderPropertyExpr(p)
  }

  def renderBooleanAnd(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  )(implicit ctxt: RenderContext): String = s" ( ${render(lhs)} ) AND ( ${render(rhs)} )"

  def renderBooleanOr(
    lhs: BooleanExpr,
    rhs: BooleanExpr
  )(implicit ctxt: RenderContext): String = s" ( ${render(lhs)} ) OR ( ${render(rhs)} )"

  def renderBooleanNot(inner: BooleanExpr)(implicit ctxt: RenderContext): String = s" NOT ( ${render(inner)} ) "

  def renderBetween(
    lhs: AtomicExpr,
    lb:  AtomicExpr,
    up:  AtomicExpr
  ): String = s" ( ${renderAtomic(lhs)} between ${renderAtomic(lb)} and ${renderAtomic(up)} ) "

  def renderComparisonPredicate(
    lhs: AtomicExpr,
    op:  ComparisonOperator,
    rhs: AtomicExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} ${sym(op)} ( ${renderAtomic(rhs)} )"

  def renderInPredicate(
    lhs: AtomicExpr,
    rhs: ValueListExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} in ${renderValueList(rhs)}"

  def renderRegexPredicate(
    lhs: AtomicExpr,
    rhs: RegexExpr
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicCasting(lhs, rhs)} ~ '${rhs.pattern}'"

  def renderLikePredicate(
    lhs:           AtomicExpr,
    rhs:           LikeExpr,
    caseSensitive: Boolean    = true
  )(implicit ctxt: RenderContext): String = if (caseSensitive) s" ${renderAtomicCasting(lhs, rhs)} like '${rhs.pattern}'"
  else s" ${renderAtomicCasting(lhs, rhs)} ilike '${rhs.pattern}'"

  def renderNullTestPredicate(
    lhs: AtomicExpr,
    is:  Boolean
  )(implicit ctxt: RenderContext): String = s" ${renderAtomic(lhs)} ${if (is) "is" else "is not"} null"

  def renderToDate(
    date: AtomicExpr,
    fmt:  AtomicExpr
  ): String = s" to_date(${renderAtomic(date)}, ${renderAtomic(fmt)}) "

  def renderLiteralBoolean(b: Boolean)(implicit ctxt: RenderContext): String = if (b) " true " else " false "

  def renderIntersects(wkt: Option[String], geometryColumn: String, bbox: Option[String]): String = {
    wkt match {
      case Some(geo) => s""" ST_Intersects( $geometryColumn, '$geo' )"""
      case _         => s""" ST_Intersects( $geometryColumn, '${bbox.getOrElse("POINT EMPTY")}' )"""
    }
  }

  def renderJsonContains(
    lhs: PropertyExpr,
    rhs: LiteralString
  )(implicit ctxt: RenderContext): String = s"${renderPropertyExpr(lhs)}::jsonb @> '${rhs.value}'::jsonb "

  def render(expr: BooleanExpr)(implicit ctxt: RenderContext): String = expr match {
    case BooleanAnd(lhs, rhs)              => renderBooleanAnd(lhs, rhs)
    case BooleanOr(lhs, rhs)               => renderBooleanOr(lhs, rhs)
    case BooleanNot(inner)                 => renderBooleanNot(inner)
    case LiteralBoolean(b)                 => renderLiteralBoolean(b)
    case ComparisonPredicate(lhs, op, rhs) => renderComparisonPredicate(lhs, op, rhs)
    case BetweenAndPredicate(lhs, lb, up)  => renderBetween(lhs, lb, up)
    case InPredicate(lhs, rhs)             => renderInPredicate(lhs, rhs)
    case RegexPredicate(lhs, rhs)          => renderRegexPredicate(lhs, rhs)
    case LikePredicate(lhs, rhs)           => renderLikePredicate(lhs, rhs, caseSensitive = true)
    case ILikePredicate(lhs, rhs)          => renderLikePredicate(lhs, rhs, caseSensitive = false)
    case NullTestPredicate(lhs, is)        => renderNullTestPredicate(lhs, is)
    case IntersectsPredicate(wkt)          => renderIntersects(wkt, ctxt.geometryColumn, ctxt.bbox)
    case JsonContainsPredicate(lhs, rhs)   => renderJsonContains(lhs, rhs)
  }

  def renderValueList(expr: ValueListExpr): String =
    s"(${expr.values.map(renderAtomic).map(_.trim).mkString(",")})"

  def cast(exp: Expr): String = exp match {
    case LiteralBoolean(_)     => "::bool"
    case LiteralNumber(_)      => "::decimal"
    case LiteralString(_)      => "::text"
    case ValueListExpr(values) => cast(values.head)
    case RegexExpr(_)          => "::text"
    case LikeExpr(_)           => "::text"
    case _                     => ""
  }

  def renderAtomicCasting(lhs: AtomicExpr, rhs: Expr): String = lhs match {
    case p @ PropertyExpr(_) => s"${renderPropertyExpr(p)}${cast(rhs)}"
    case _                   => s"${renderAtomic(lhs)}${cast(rhs)}"
  }

  def sym(op: ComparisonOperator): String = op match {
    case EQ  => " = "
    case NEQ => " != "
    case LT  => " < "
    case GT  => " > "
    case LTE => " <= "
    case GTE => " >= "
  }

}
