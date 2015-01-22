package querylang

import play.api.libs.json.JsValue

import scala.util.{Success, Failure, Try}

/**
 * Renders a Boolean expression to a repository specific query object
 * Created by Karel Maesen, Geovise BVBA on 22/01/15.
 */

class QueryRenderException(msg: String) extends RuntimeException

trait RenderDelegate[T] {

  def renderNeg(inner: Try[T]): Try[T]

  def renderAnd(lhs: Try[T], rhs: Try[T]): Try[T]

  def renderOr(lhs: Try[T], rhs: Try[T]): Try[T]

  def renderComparison(lhs: Try[T], op: ComparisonOperator, rhs: Try[T]) : Try[T]

  def renderLiteralString(s: String) : Try[T]

  def renderLiteralNumber(n: BigDecimal) : Try[T]

  def renderPropertyName(p: String) : Try[T]

  def renderBooleanLiteral(b: Boolean): Try[T]

  def renderBooleanValueExpression(b: Boolean) : Try[T]

  def renderComparisonOp(op: ComparisonOperator) : Try[T]

  protected def sequence(left : Try[T], right: Try[T]) : Try[(T,T)] = (left, right) match {
    case (Success(l), Success(r)) => Success((l,r))
    case ( Failure(t1), _ ) => Failure(t1)
    case ( _ , Failure(t2)) => Failure(t2)
   }

}

class QueryRenderer[T](delegate: RenderDelegate[T]) {

  def render(expr: BooleanExpr) : Try[T] = expr match {
    case ComparisonPredicate(lhs, op, rhs) => delegate.renderComparison(renderValue(lhs), op, renderValue(rhs))
    case BooleanOr(lhs, rhs) => delegate.renderOr(render(lhs), render(rhs))
    case BooleanAnd(rhs, lhs) => delegate.renderAnd(render(rhs), render(lhs))
    case BooleanNot(inner) => delegate.renderNeg(render(inner))
    case LiteralBoolean(inner) => delegate.renderBooleanValueExpression(inner)
  }

  def renderValue(valExpr: ValueExpr) : Try[T] = valExpr match {
    case LiteralString(s) => delegate.renderLiteralString(s)
    case LiteralNumber(n) => delegate.renderLiteralNumber(n)
    case PropertyName(p) => delegate.renderPropertyName(p)
    case LiteralBoolean(b) => delegate.renderBooleanLiteral(b)
    case _ => Failure(new IllegalArgumentException(s"Not supported expression ${valExpr.toString}"))
  }

}
