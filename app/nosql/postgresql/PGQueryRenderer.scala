package nosql.postgresql

import querylang._

import scala.util.{Failure, Success, Try}

/**
* Created by Karel Maesen, Geovise BVBA on 23/01/15.
*/
object PGQueryRenderer extends QueryRenderer[String] {

  override def render(expr: BooleanExpr): Try[String] = Try {
      rootRender(expr)
  }

  def rootRender(expr: BooleanExpr) : String = expr match {
    case BooleanAnd(lhs, rhs) => s" ( ${rootRender(lhs)} ) AND ( ${rootRender(rhs)} )"
    case BooleanOr(lhs, rhs) => s" ( ${rootRender(lhs)} ) OR ( ${rootRender(rhs)} )"
    case BooleanNot(inner) => s" NOT ( ${rootRender(inner)} ) "
    case LiteralBoolean(b) => if (b) " true " else " false "
    case ComparisonPredicate(lhs, op, rhs ) => s" ${renderPropertyExpr(lhs,rhs)} ${sym(op)} ( ${renderValue(rhs)} )"
  }

  private def renderPropertyExpr(lhs: PropertyExpr, rhs: ValueExpr): String = {
    val variadicPath = "'" + lhs.path.replaceAll("\\.", "','") + "'"
    val cast = rhs match {
      case LiteralBoolean(_) => "bool"
      case LiteralNumber(_)  => "decimal"
      case LiteralString(_)  => "text"
    }
    s"json_extract_path_text(json, $variadicPath)::$cast"
  }

  private def renderValue(expr: ValueExpr) : String = expr match{
    case LiteralBoolean(b) => if (b) " true " else " false "
    case LiteralNumber(n)  => s" ${n.toString} "
    case LiteralString(s)  => s" '$s' "
  }

  private def sym(op: ComparisonOperator): String = op match {
      case EQ => " = "
      case NEQ => " != "
      case LT => " < "
      case GT => " > "
      case LTE => " <= "
      case GTE => " >= "
    }
}
