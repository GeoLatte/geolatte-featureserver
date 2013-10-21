package org.geolatte.nosql.mongodb

import org.specs2.mutable._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.Envelope
import nosql.json.EnvelopeSerializer

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/10/13
 */
class EnvelopeSerializerTest extends Specification {

  val wgs84 = CrsId.valueOf(4326)

  "A regular envelope " should {
    val env = new Envelope(-32, 12.34, 40.0, 33.12345, wgs84)

      "be serialized as 'srid:minx, miny,maxx, maxy'" in  {
        EnvelopeSerializer(env) must_== "4326:-32.0,12.34,40.0,33.12345"
      }

      "be deserialized from '-32,12.34,40.0,33.12345' " in {
        EnvelopeSerializer.unapply("4326:-32,12.34,40.0,33.12345") must beSome( env )
      }
    }

  "a null envelope " should {
    val env = null

    " be serialized to empty string" in {
      EnvelopeSerializer(env) must_== ""
    }

  }


}
