package persistence.postgresql

import org.specs2.mutable.Specification
import persistence.{ ASC, DESC, FldSortSpec }

/**
 * Created by Karel Maesen, Geovise BVBA on 06/11/2019.
 */
class PGExpressionSpecs extends Specification with PGExpression {

  "PGExpression" should {
    "split a field expression to a Seq " in {
      fldSpecToComponents("properties.a.b.c") must contain("properties", "a", "b")
    }

    "intersperseOps intersperses components with the correct operator" in {
      (intersperseOperators(Seq("properties", "a", "b"), "->") === "properties->a->b") and (
        (intersperseOperators(Seq("properties", "a"), "->") === "properties->a") and (
          intersperseOperators(Seq("properties"), "->") === "properties"
        )
      )
    }

    "intersperseOps intersperses components with the correct operator, also if last operator should be different" in {
      (intersperseOperators(Seq("properties", "a", "b"), "->", Some("->>")) === "properties->a->>b") and (
        (intersperseOperators(Seq("properties", "a"), "->", Some("->>")) === "properties->>a") and (
          intersperseOperators(Seq("properties"), "->", Some("->>")) === "properties"
        )
      )
    }

    "render a field expression using -> and ->> " in {
      fldSortSpecToSortExpr(FldSortSpec("properties.a.b.c", ASC)) === "( json->'properties'->'a'->'b'->>'c' ) ASC" and (
        fldSortSpecToSortExpr(FldSortSpec("single", DESC)) === "( json->>'single' ) DESC"
      )
    }
  }

}
