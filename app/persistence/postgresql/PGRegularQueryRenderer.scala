package persistence.postgresql

import persistence.querylang._
import PGSqlUtils.safeIdentifier

/**
 * Created by Karel Maesen, Geovise BVBA on 16/04/16.
 */
object PGRegularQueryRenderer extends BaseQueryRenderer {

  // A property path can only reach us through one of the parsers, and neither
  // admits a double quote in a name. Quoting through `safeIdentifier` rather
  // than by hand keeps that from being the only thing standing between a path
  // and SQL statement context, should a grammar ever widen.
  override def renderPropertyExpr(expr: PropertyExpr): String = {
    val withoutPrefix =
      if (expr.path.trim.startsWith("properties.")) {
        expr.path.trim.substring(11)
      } else {
        expr.path
      }
    safeIdentifier(withoutPrefix)
  }

  override def cast(exp: Expr): String = "" // don't cast

}
