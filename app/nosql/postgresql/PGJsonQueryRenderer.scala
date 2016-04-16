package nosql.postgresql

import querylang._

import scala.util.{Failure, Success, Try}

/**
* Created by Karel Maesen, Geovise BVBA on 23/01/15.
*/
object PGJsonQueryRenderer extends AbstractPGQueryRenderer {

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

  protected def renderPropertyExpr(lhs: PropertyExpr, rhs: Expr): String = {
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
    


}
