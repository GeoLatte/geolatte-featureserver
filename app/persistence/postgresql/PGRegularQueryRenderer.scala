package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
object PGRegularQueryRenderer extends AbstractPGQueryRenderer {

  override def render(expr: BooleanExpr): String = expr match {
    case BooleanAnd(lhs, rhs) => s" ( ${render(lhs)} ) AND ( ${render(rhs)} )"
    case BooleanOr(lhs, rhs) => s" ( ${render(lhs)} ) OR ( ${render(rhs)} )"
    case BooleanNot(inner) => s" NOT ( ${render(inner)} ) "
    case LiteralBoolean(b) => if (b) " true " else " false "
    case ComparisonPredicate(lhs, op, rhs) => s" ${renderPropertyExpr(lhs)} ${sym(op)} ( ${renderValue(rhs)} )"
    case InPredicate(lhs, rhs) => s" ${renderPropertyExpr(lhs)} in ${renderValueList(rhs)}"
    case RegexPredicate(lhs, rhs) => s" ${renderPropertyExpr(lhs)} ~ '${rhs.pattern}'"
    case LikePredicate(lhs, rhs) => s" ${renderPropertyExpr(lhs)} ilike '${rhs.pattern}'"
    case NullTestPredicate(lhs, is) => s" ${renderPropertyExpr(lhs)} ${if (is) "is" else "is not"} null"
  }

  protected def renderPropertyExpr(lhs: PropertyExpr): String =
    if (lhs.path.trim.startsWith("properties.")) lhs.path.trim.substring(11)
    else lhs.path

}
