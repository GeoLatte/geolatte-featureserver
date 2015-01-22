package nosql.mongodb

import play.api.libs.json._
import querylang._

import scala.util.{Failure, Success, Try}

/**
 * Renders a boolean expression to a MongoDB Query Document
 * Created by Karel Maesen, Geovise BVBA on 22/01/15.
 */

object MongoDBRenderDelegate extends RenderDelegate[JsValue] {


  private def unpack(v : Try[JsValue]) : String = v match {
    case Success(JsString(s)) => s
    case _ => throw new QueryRenderException(s"Can't unpack ${v.toString} to String")
  }

  override def renderComparison( lhs: Try[JsValue], op: ComparisonOperator, rhs: Try[JsValue]) : Try[JsValue] =
    Try {
      val args: (String, JsValue) = (lhs, rhs) match {
        case (Success(JsString(s)), Success(rv)) => (s, rv)
        case _ => throw new QueryRenderException("Failure to interpret arguments")
      }


      (op, args) match {
        case (EQ, (p, v)) => Json.obj(p -> v)
        case (o, (p,v))  => Json.obj(  unpack(renderComparisonOp(o)) -> Json.arr(p,v))
        case _ => throw new QueryRenderException(s"Not supported expression ${op.toString}")
      }
    }



  override def renderLiteralString(s: String): Try[JsValue] = Success(JsString(s))

  override def renderPropertyName(p: String): Try[JsValue] = Success(JsString(p))

  override def renderLiteralNumber(n: BigDecimal): Try[JsValue] = Success(JsNumber(n))

  override def renderComparisonOp(op : ComparisonOperator) : Try[JsValue] = Try {
    op match {
      case GT =>  JsString("$gt")
      case GTE => JsString("$gte")
      case LT =>  JsString("$lt")
      case LTE => JsString("$lte")
      case NEQ => JsString("$ne")
      case EQ => throw new QueryRenderException(s"EQ has no MongoDB operator")
    }
  }

  override def renderNeg(inner: Try[JsValue]): Try[JsValue] = inner.map{
    expr => Json.obj("$not" -> expr)
  }

  override def renderOr(lhs: Try[JsValue], rhs: Try[JsValue]): Try[JsValue] = sequence(lhs, rhs) map {
    case (l,r) => Json.obj("$or" -> Json.arr(l,r))
  }

  override def renderAnd(lhs: Try[JsValue], rhs: Try[JsValue]): Try[JsValue] = sequence(lhs, rhs) map {
    case(l,r) => Json.obj("$and" -> Json.arr(l,r))
  }

  override def renderBooleanLiteral(b: Boolean): Try[JsValue] = Success(JsBoolean(b))

  override def renderBooleanValueExpression(b: Boolean): Try[JsValue] = Success(Json.obj())

}

object MongoDBQueryRenderer extends QueryRenderer[JsValue](MongoDBRenderDelegate)








