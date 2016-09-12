package featureserver.postgresql

import querylang.{ GTE, LTE, _ }

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
abstract class AbstractPGQueryRenderer extends QueryRenderer[String] {

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
