package persistence.json

import controllers.Formats
import persistence.Metadata
import org.specs2.mutable.Specification
import play.api.libs.json.Json

/**
 * Created by Karel Maesen, Geovise BVBA on 06/02/15.
 */
class MetadataFormatSpec extends Specification {

  "MetadataReaders " should {

    "validate " in {

      val jsMd = Json.obj(
        "extent" -> Json.obj(
          "crs" -> 31370,
          "envelope" -> Json.arr(0, 0, 300000, 300000)),
        "index-level" -> 8,
        "id-type" -> "decimal")

      val md = jsMd.as[Metadata](Formats.CollectionReadsForJsonTable)

      md must beAnInstanceOf[Metadata]

    }

  }

}