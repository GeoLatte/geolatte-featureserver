package utilities

import org.specs2.mutable.Specification
import play.api.libs.json._

/**
 * Created by Karel Maesen, Geovise BVBA on 21/11/14.
 */
class JsonHelperSpec extends Specification {

  "The JsonHelper.flatten " should {

    val obj = Json.obj(
      "id" -> 1,
      "att" -> "value",
      "nested" -> Json.obj("foo" -> "bar"),
      "nested2" -> Json.obj("foo" -> Json.obj("bar" -> 42)),
      "arr" -> Json.arr(1, 2, 3, 4)
    )

    "strip contain nested keys, but not arrays " in {
      JsonHelper.flatten(obj) must containTheSameElementsAs(Seq(
        ("id", JsNumber(1)), ("att", JsString("value")), ("nested.foo", JsString("bar")), ("nested2.foo.bar", JsNumber(42))
      ))
    }

  }
}
