package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
object PGRegularQueryRenderer extends BaseQueryRenderer {

  override def renderPropertyExpr(expr: PropertyExpr): String = {
    val withoutPrefix =
      if (expr.path.trim.startsWith("properties.")) {
        expr.path.trim.substring(11)
      } else {
        expr.path
      }
    s""""$withoutPrefix""""
  }

  override def cast(exp: Expr): String = "" // don't cast

}
