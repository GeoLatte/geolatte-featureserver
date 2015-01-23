package querylang

import play.api.libs.json.JsValue

import scala.util.{Success, Failure, Try}

class QueryRenderException(msg: String) extends RuntimeException

/**
 * Renders a Boolean expression to a repository specific query object
 * Created by Karel Maesen, Geovise BVBA on 22/01/15.
 */
trait QueryRenderer[T]{

  def render(expr: BooleanExpr) : Try[T]

}
