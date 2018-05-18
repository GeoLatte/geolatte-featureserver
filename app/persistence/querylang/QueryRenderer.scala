package persistence.querylang

/**
 * Renders a Boolean expression to a repository specific query object
 * Created by Karel Maesen, Geovise BVBA on 22/01/15.
 */
trait QueryRenderer[T, C] {
  def render(expr: BooleanExpr)(implicit context: C): T
}
