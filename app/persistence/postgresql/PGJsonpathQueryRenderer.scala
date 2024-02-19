package persistence.postgresql

import persistence.querylang._

object PGJsonpathQueryRenderer extends BaseQueryRenderer {

  override def renderComparisonPredicate(
                                          lhs: AtomicExpr,
                                          op: ComparisonOperator,
                                          rhs: AtomicExpr
                                        )(implicit ctxt: RenderContext): String = {
    s" json @?? '${jsonpathExpr(lhs, op, rhs)}'"
  }

  def jsonpathExpr(
                    lhs: AtomicExpr,
                    op: ComparisonOperator,
                    rhs: AtomicExpr
                  ): String = {
    s"${renderAtomic(lhs)} ? ( @ ${sym(op)} ${renderAtomic(rhs)})"
  }

  override def renderAtomic(expr: AtomicExpr): String = expr match {
    case ToDate(date, fmt) => renderToDate(date, fmt)
    case LiteralBoolean(b) => if (b) " true " else " false "
    case LiteralNumber(n) => s" ${n.toString} "
    case LiteralString(s) => s" \"$s\" "
    case p@PropertyExpr(_) => renderPropertyExpr(p)
  }

  def renderPropertyExpr(expr: PropertyExpr): String = {
    s"$$.${expr.path}"
  }

  // FIXME
  def propertyPathAsJsonText(expr: PropertyExpr): String = {
    s"$$.${expr.path}"
  }

  override def renderNullTestPredicate(
                                        lhs: AtomicExpr,
                                        is:  Boolean
                                      )(implicit ctxt: RenderContext): String =
    if (is) {
      s" json @?? '${renderAtomic(lhs)} ? (@ ${sym(EQ)} null)'"
    } else {
      s" json @?? '${renderAtomic(lhs)} ? (@ ${sym(NEQ)} null)'"
    }

  override def renderToDate(
                             date: AtomicExpr,
                             fmt:  AtomicExpr
                           ): String =
    s"${renderAtomic(date)}.datetime(${renderAtomic(fmt)})"

  override def sym(op: ComparisonOperator): String = op match {
    case EQ => " == "
    case NEQ => " != "
    case LT => " < "
    case GT => " > "
    case LTE => " <= "
    case GTE => " >= "
  }

  override def renderRegexPredicate(
                                     lhs: AtomicExpr,
                                     rhs: RegexExpr
                                   )(implicit ctxt: RenderContext): String =
    s" json @?? '${renderAtomic(lhs)} ? (@ like_regex \"${rhs.pattern}\")'"

  override def renderLikePredicate(
                                    lhs: AtomicExpr,
                                    rhs: LikeExpr,
                                    caseSensitive: Boolean = true
                                  )(implicit ctxt: RenderContext): String =
    PGJsonbFallbackQueryRenderer.renderLikePredicate(lhs, rhs, caseSensitive)

  override def renderInPredicate(
                                  lhs: AtomicExpr,
                                  rhs: ValueListExpr
                                )(implicit ctxt: RenderContext): String =
    PGJsonbFallbackQueryRenderer.renderInPredicate(lhs, rhs)

  override def renderJsonContains (
                                    lhs: PropertyExpr,
                                    rhs: LiteralString
                                  )(implicit ctxt: RenderContext): String =
    s"${PGJsonbFallbackQueryRenderer.renderPropertyExpr(lhs)} @> '${rhs.value}'::jsonb "

  override def renderBetween(
                              lhs: AtomicExpr,
                              lb: AtomicExpr,
                              up: AtomicExpr
                            ): String =
    PGJsonbFallbackQueryRenderer.renderBetween(lhs, lb, up)
}

/**
 * This query renderer is used when a query can not be rendered as a jsonpath expression
 */
object PGJsonbFallbackQueryRenderer extends BaseQueryRenderer with PGExpression {

  def renderPropertyExpr(expr: PropertyExpr): String = {
    val quotedFields = fldSpecToComponents(expr.path).map(quote)
    val flds = "json" +: quotedFields
    s"( ${intersperseOperators(flds, "->")} )"
  }

  def propertyPathAsJsonText(expr: PropertyExpr): String = {
    val quotedFields = fldSpecToComponents(expr.path).map(quote)
    val flds = "json" +: quotedFields
    s"( ${intersperseOperators(flds, "->", Some("->>"))} )"
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

  override def renderBetween(lhs: AtomicExpr, lb: AtomicExpr, up: AtomicExpr): String =
    s" ( ${renderAtomicCasting(lhs, lb)} between ${renderAtomic(lb)} and ${renderAtomic(up)} ) "

  override def renderToDate(
                             date: AtomicExpr,
                             fmt:  AtomicExpr
                           ): String = s" to_date(${renderAtomicPropsAsText(date)}, ${renderAtomicPropsAsText(fmt)}) "

}

