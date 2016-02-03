package nosql.postgresql

import querylang._

/**
* Created by Karel Maesen, Geovise BVBA on 23/01/15.
*/
object PGQueryRenderer extends QueryRenderer[String] {

  def render(expr: BooleanExpr) : String = expr match {
    case BooleanAnd(lhs, rhs) => s" ( ${render(lhs)} ) AND ( ${render(rhs)} )"
    case BooleanOr(lhs, rhs) => s" ( ${render(lhs)} ) OR ( ${render(rhs)} )"
    case BooleanNot(inner) => s" NOT ( ${render(inner)} ) "
    case LiteralBoolean(b) => if (b) " true " else " false "
    case ComparisonPredicate(lhs, op, rhs ) => s" ${renderPropertyExpr(lhs,rhs)} ${sym(op)} ( ${renderValue(rhs)} )"
    case InPredicate(lhs, rhs) =>  s" ${renderPropertyExpr(lhs,rhs)} in ${renderValueList(rhs)}"
    case RegexPredicate(lhs, rhs) => s" ${renderPropertyExpr(lhs,rhs)} ~ '${rhs.pattern}'"
    case LikePredicate(lhs, rhs) => s" ${renderPropertyExpr(lhs,rhs)} ilike '${rhs.pattern}'"
  }

  private def renderPropertyExpr(lhs: PropertyExpr, rhs: Expr): String = {
    val variadicPath: String = path2VariadicList(lhs)
    s"json_extract_path_text(json, $variadicPath)::${cast(rhs)}"
  }


  def cast(exp: Expr) : String = exp match {
    case LiteralBoolean(_) => "bool"
    case LiteralNumber(_)  => "decimal"
    case LiteralString(_)  => "text"
    case ValueListExpr( values ) => cast( values.head )
    case RegexExpr( _ ) => "text"
    case LikeExpr( _ ) =>"text"
    case _ => "text"
  }

  def path2VariadicList(propertyExpr: PropertyExpr): String = "'" + propertyExpr.path.replaceAll("\\.", "','") + "'"
    

  private def renderValue(expr: ValueExpr) : String = expr match{
    case LiteralBoolean(b) => if (b) " true " else " false "
    case LiteralNumber(n)  => s" ${n.toString} "
    case LiteralString(s)  => s" '$s' "
  }

  private def renderValueList(expr: ValueListExpr) : String =
    s"(${expr.values.map(renderValue).map(_.trim).mkString(",")})"

  private def sym(op: ComparisonOperator): String = op match {
      case EQ => " = "
      case NEQ => " != "
      case LT => " < "
      case GT => " > "
      case LTE => " <= "
      case GTE => " >= "
    }
}
