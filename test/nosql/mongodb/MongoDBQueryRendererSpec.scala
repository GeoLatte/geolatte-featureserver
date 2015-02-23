package nosql.mongodb

import org.specs2.mutable.Specification
import play.api.libs.json.Json

import querylang._

/**
 * Created by Karel Maesen, Geovise BVBA on 22/01/15.
 */
class MongoDBQueryRendererSpec extends Specification {

  "The MongoDBQueryRenderer " should {

    val renderer = MongoDBQueryRenderer

    "properly render boolean expressions containing equality expresssions " in {
      val expr: BooleanExpr = QueryParser.parse("ab.cd = 12").get
        renderer.render(expr) ==== Json.obj("ab.cd" -> 12 )
    }

    "properly render comparison expressions contain boolean literal" in {
      val expr = QueryParser.parse("ab.cd = TRUE").get
      renderer.render(expr) === Json.obj("ab.cd" -> true)
    }

    "properly render simple equality expression " in {
      val expr = QueryParser.parse("properties.foo = 'bar1'").get
      renderer.render(expr) === Json.obj("properties.foo" -> "bar1")
    }


    "properly render boolean expressions containing comparison expresssions other than equality " in {

      val expr1 = QueryParser.parse("ab.cd > 12").get
      val expr2 = QueryParser.parse("ab.cd >= 12").get
      val expr3 = QueryParser.parse("ab.cd != 'abc'").get

      (
        renderer.render(expr1) ==== Json.obj( "$gt"  -> Json.arr("ab.cd" ,12 ))
        ) and (
        renderer.render(expr2) === Json.obj( "$gte"  -> Json.arr("ab.cd" ,12 ))
        ) and (
        renderer.render(expr3) === Json.obj( "$ne"  -> Json.arr("ab.cd" ,"abc" ))
        )
    }

    "properly render negated expressions " in {
      val expr1 = QueryParser.parse("not ab.cd = 12").get
      renderer.render(expr1) === Json.obj( "$not" -> Json.obj("ab.cd" -> 12))
    }

    "properly render AND expressions " in {
      val expr1 = QueryParser.parse(" (ab = 12) and (cd > 'c') ").get
      renderer.render(expr1) ===
        Json.obj( "$and" -> Json.arr(Json.obj("ab" -> 12), Json.obj("$gt" -> Json.arr("cd", "c"))))
    }

    "properly render OR expressions " in {
      val expr1 = QueryParser.parse(" (ab = 12) or (cd > 'c') ").get
      renderer.render(expr1) ===
        Json.obj( "$or" -> Json.arr(Json.obj("ab" -> 12), Json.obj("$gt" -> Json.arr("cd", "c"))))
    }

    "properly render a boolean literal as a boolean expression" in {
      val expr = QueryParser.parse("TRUE").get
      renderer.render(expr) === Json.obj()
    }

    "property render an IN predicate" in {
      val expr1 = QueryParser.parse(" a.b in (1,2,3) ").get
      renderer.render(expr1) ===
        Json.obj(
          "a.b" -> Json.obj(
            "$in" -> Json.arr(3,2,1)
          )
        )
    }

  }

}
