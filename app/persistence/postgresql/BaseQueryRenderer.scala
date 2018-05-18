package persistence.postgresql

import persistence.querylang.{ GTE, LTE, _ }

case class RenderContext(geometryColumn: String, bbox: Option[String] = None)

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
trait BaseQueryRenderer extends QueryRenderer[String, RenderContext] {

  def renderBooleanAnd(lhs: BooleanExpr, rhs: BooleanExpr)(implicit ctxt: RenderContext): String

  def renderBooleanOr(lhs: BooleanExpr, rhs: BooleanExpr)(implicit ctxt: RenderContext): String

  def renderBooleanNot(inner: BooleanExpr)(implicit ctxt: RenderContext): String

  def renderLiteralBoolean(b: Boolean)(implicit ctxt: RenderContext): String = if (b) " true " else " false "

  def renderComparisonPredicate(lhs: PropertyExpr, op: ComparisonOperator, rhs: ValueExpr)(implicit ctxt: RenderContext): String

  def renderInPredicate(lhs: PropertyExpr, rhs: ValueListExpr)(implicit ctxt: RenderContext): String

  def renderRegexPredicate(lhs: PropertyExpr, rhs: RegexExpr)(implicit ctxt: RenderContext): String

  def renderLikePredicate(lhs: PropertyExpr, rhs: LikeExpr)(implicit ctxt: RenderContext): String

  def renderNullTestPredicate(lhs: PropertyExpr, is: Boolean)(implicit ctxt: RenderContext): String

  def renderIntersects(wkt: Option[String], geometryColumn: String, bbox: Option[String]): String = {
    wkt match {
      case Some(geo) => s""" ST_Intersects( $geometryColumn, '${geo}' )"""
      case _ => s""" ST_Intersects( $geometryColumn, '${bbox.getOrElse("POINT EMPTY")}' )"""
    }
  }

  def render(expr: BooleanExpr)(implicit ctxt: RenderContext): String = expr match {
    case BooleanAnd(lhs, rhs) => renderBooleanAnd(lhs, rhs)
    case BooleanOr(lhs, rhs) => renderBooleanOr(lhs, rhs)
    case BooleanNot(inner) => renderBooleanNot(inner)
    case LiteralBoolean(b) => renderLiteralBoolean(b)
    case ComparisonPredicate(lhs, op, rhs) => renderComparisonPredicate(lhs, op, rhs)
    case InPredicate(lhs, rhs) => renderInPredicate(lhs, rhs)
    case RegexPredicate(lhs, rhs) => renderRegexPredicate(lhs, rhs)
    case LikePredicate(lhs, rhs) => renderLikePredicate(lhs, rhs)
    case NullTestPredicate(lhs, is) => renderNullTestPredicate(lhs, is)
    case IntersectsPredicate(wkt) => renderIntersects(wkt, ctxt.geometryColumn, ctxt.bbox)
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
