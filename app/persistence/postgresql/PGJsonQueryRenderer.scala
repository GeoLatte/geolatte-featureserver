package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 23/01/15.
 */
object PGJsonQueryRenderer extends BaseQueryRenderer {

  def renderPropertyExpr(expr: PropertyExpr) = {
    val variadicPath = path2VariadicList(expr)
    s"json_extract_path(json, $variadicPath)"
  }

  def propertyPathAsJsonText(expr: PropertyExpr): String = {
    val variadicPath = path2VariadicList(expr)
    s"json_extract_path_text(json, $variadicPath)"
  }

  def renderAtomicPropsAsText(expr: AtomicExpr): String = expr match {
    case p @ PropertyExpr(_) => propertyPathAsJsonText(p)
    case _                   => super.renderAtomic(expr)
  }

  override def renderAtomicCasting(lhs: AtomicExpr, rhs: Expr): String = lhs match {
    case p @ PropertyExpr(_) => s"${propertyPathAsJsonText(p)}${cast(rhs)}"
    case _                   => s"${renderAtomic(lhs)}${cast(rhs)}"
  }

  override def renderNullTestPredicate(
    lhs: AtomicExpr,
    is:  Boolean
  )(implicit ctxt: RenderContext): String = s" ${renderAtomicPropsAsText(lhs)} ${
    if (is) {
      "is"
    } else {
      "is not"
    }
  } null"

  override def renderToDate(
    date: AtomicExpr,
    fmt:  AtomicExpr
  ): String = s" to_date(${renderAtomicPropsAsText(date)}, ${renderAtomicPropsAsText(fmt)}) "

  private def renderPropertyExprwithoutCast(lhs: PropertyExpr): String = {
    val variadicPath: String = path2VariadicList(lhs)
    s"json_extract_path_text(json, $variadicPath)"
  }

  private def path2VariadicList(propertyExpr: PropertyExpr): String =
    "'" + propertyExpr.path.replaceAll("\\.", "','") + "'"

}
