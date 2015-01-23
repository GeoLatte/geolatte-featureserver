package nosql.mongodb

import play.api.libs.json._
import querylang._

import scala.util.{Failure, Success, Try}


object MongoDBQueryRenderer extends QueryRenderer[JsValue]{

  override def render(expr: BooleanExpr): Try[JsValue] = Try { renderRoot(expr) }

  def op2String(op: ComparisonOperator) : String = op match {
    case GT =>  "$gt"
    case GTE => "$gte"
    case LT =>  "$lt"
    case LTE => "$lte"
    case NEQ => "$ne"
    case EQ => "$eq"
  }

  def renderRoot(expr: BooleanExpr) : JsValue  = expr match {
    case BooleanOr(lhs, rhs) => Json.obj( "$or" -> Json.arr(renderRoot(lhs), renderRoot(rhs)))
    case BooleanAnd(lhs, rhs) => Json.obj("$and" -> Json.arr(renderRoot(lhs), renderRoot(rhs)))
    case BooleanNot(inner) => Json.obj("$not" -> renderRoot(inner))
    case ComparisonPredicate(lhs, op, rhs) => op match {
      case EQ => Json.obj( lhs.path -> renderValue(rhs))
      case _ => Json.obj(op2String(op) -> Json.arr( lhs.path, renderValue(rhs)))
    }
    case LiteralBoolean(b) => Json.obj()
  }

  def renderValue(valueExpr : ValueExpr) : JsValue = valueExpr match {
    case LiteralString(s) => JsString(s)
    case LiteralNumber(n) => JsNumber(n)
    case LiteralBoolean(b) => JsBoolean(b)
  }

}








