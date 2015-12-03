package nosql.mongodb

import play.api.libs.json._
import querylang._


object MongoDBQueryRenderer extends QueryRenderer[JsValue] {


  def op2String(op: ComparisonOperator): String = op match {
    case GT => "$gt"
    case GTE => "$gte"
    case LT => "$lt"
    case LTE => "$lte"
    case NEQ => "$ne"
    case EQ => "$eq"
  }

  override def render(expr: BooleanExpr): JsValue = expr match {
    case BooleanOr(lhs, rhs) => Json.obj("$or" -> Json.arr(render(lhs), render(rhs)))
    case BooleanAnd(lhs, rhs) => Json.obj("$and" -> Json.arr(render(lhs), render(rhs)))
    case BooleanNot(inner) => Json.obj("$not" -> render(inner))
    case ComparisonPredicate(lhs, op, rhs) => op match {
      case EQ => Json.obj(lhs.path -> renderValue(rhs))
      case _ => Json.obj(op2String(op) -> Json.arr(lhs.path, renderValue(rhs)))
    }
    case InPredicate(lhs, rhs) => Json.obj(
      lhs.path ->
        Json.obj("$in" ->
          renderValue(rhs)
        )
    )
    case RegexPredicate(lhs, rhs) => Json.obj(
      lhs.path ->
        Json.obj("$regex" -> rhs.pattern) //TODO -- this is not tested !!
    )
    case LikePredicate(lhs, rhs) => Json.obj(
      lhs.path ->
        Json.obj("$regex" -> like2regex(rhs.pattern), "$options" -> "i") //TODO -- escaping is nog properly handled yet
    )
    case LiteralBoolean(b) => Json.obj()
  }

  def renderValue(valueExpr: ValueExpr): JsValue = valueExpr match {
    case LiteralString(s) => JsString(s)
    case LiteralNumber(n) => JsNumber(n)
    case LiteralBoolean(b) => JsBoolean(b)
  }

  def renderValue(valueListExpr: ValueListExpr): JsArray = JsArray(valueListExpr.values.map(renderValue))

  def like2regex(pattern: String): String =  {

    val SpecialChars : Map[Char, String] = Map(
      '%' -> ".*",
      '_' -> ".",
      '.' -> "\\.",
      '^' -> "\\^",
      '$' -> "\\$",
      '[' -> "\\[",
      ']' -> "\\]"
    )

    pattern.foldLeft[(Boolean, StringBuilder)]((false, new StringBuilder())) { (state, char) =>
      state match {
        case (true, bld) => (false, bld.append(char))
        case (false, bld) if  char == '\\'   => (true, bld)
        case (false, bld) if SpecialChars.keySet.contains(char) => (false, bld.append(SpecialChars(char)))
        case (false, bld) => (false, bld.append(char))
      }
    }._2.toString
  }



}








