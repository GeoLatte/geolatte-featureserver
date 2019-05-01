package persistence.postgresql

import persistence.querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
object PGRegularQueryRenderer extends BaseQueryRenderer {

  override def renderPropertyExpr(expr: PropertyExpr): String = if (expr.path.trim.startsWith("properties.")) expr.path.trim.substring(11)
  else expr.path

  override def cast(exp: Expr): String = "" // don't cast

}
