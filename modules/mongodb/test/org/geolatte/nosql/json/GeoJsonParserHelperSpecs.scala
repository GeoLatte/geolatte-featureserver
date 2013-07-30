package org.geolatte.nosql.json

import org.specs2.mutable.Specification
import org.codehaus.jackson.JsonFactory
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import org.geolatte.common.Feature
import scala.annotation.tailrec

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/29/13
 */
class GeoJsonParserHelperSpecs extends Specification {

  lazy val codec = (new JsonMapper()).getObjectMapper
  lazy val factory = new JsonFactory(codec)

  def mkTestJsonParserHelper(input: String, state: ParserStates.Value = ParserStates.START) = {
    val bytes = input.getBytes("UTF-8")
    (bytes, GeoJsonParserHelper(factory.createJsonParser(bytes), state))
  }

  "If the features property exists in the input, the helper " should {

    val (bytes, jph) = mkTestJsonParserHelper( """ { "p1" : 1, "features" : [{ "type": "Feature", "properties": { "module": "TGR01007", "tlid": 1 }, "geometry": { "type": "Point", "coordinates": [-87.067872, 33.093221] }}] """)

    "begin in state START" in {
      jph.currentState must_== ParserStates.START
    }

    "first move to state FEATURES_PROPERTY_FOUND " in {
      jph.next
      jph.currentState must_== ParserStates.FEATURES_PROPERTY_FOUND
    }

    "then move to the start of the feature array" in {
      jph.next
      jph.currentState must_== ParserStates.IN_FEATURES_ARRAY
    }

    "at which point a feature can be read " in {
      (jph.currentState must_== ParserStates.READ_FEATURE) and (jph.readFeatures.length must_== 1)

    }

    "after which the end of the features array is read" in {
      jph.next
      jph.currentState must_== ParserStates.END_FEATURES_ARRAY
    }

    "further skipping leads no change in state and atEnd property set to true" in {
      while(!jph.atEnd) jph.next
      jph.next
      (jph.currentState must_== ParserStates.END_FEATURES_ARRAY) and (jph.atEnd must_== true)
    }



  }

  "If the features property does not exist in the input, the helper " should {

    val (bytes, jph) = mkTestJsonParserHelper( """ { "p1" : 1, "p2" : [1,2,3] } """)
    jph.next

    "remain in state START" in {
      jph.currentState must_== ParserStates.START
    }

    "with nu currentToken" in {
      jph.currentToken must_== null
    }

    "point beyond the input array" in {
      jph.currentLocation must be_>=(bytes.size)
    }

    "and atEnd set to true" in {
      jph.atEnd must_== true
    }

  }

  "On skipping to a property that does not exist in the input when input is not a complete structure, the helper " should {

    val (bytes, jph) = mkTestJsonParserHelper( """ { "p1" : 1, "p2" : [1,2,3], "p3": { "sub """)
    val result = jph.next

    "remain int state START " in {
      jph.currentState must_== ParserStates.START
    }

    "point to the last known valid token " in {
      val offset = jph.currentLocation
      offset must_== 35
      new String(bytes.slice(offset, offset + 1)) must beEqualTo("{")
    }

    "if subsequently the structure is completed, the parser must be able to continue" in {

      val nextBytes = bytes.slice(jph.currentLocation, bytes.length) ++ """type": 2}, features : """.getBytes("UTF-8")
      val jph2 = GeoJsonParserHelper(factory.createJsonParser(nextBytes), jph.currentState)
      jph2.next
      jph2.currentState = ParserStates.FEATURES_PROPERTY_FOUND
    }
  }



  "On a complete FeatureCollection Json, the helper " should {

    val inputStr = """{ "meta": "test", "features": [
                      { "type": "Feature", "properties": { "module": "TGR01007", "tlid": 1 }, "geometry": { "type": "Point", "coordinates": [-87.067872, 33.093221] }},
                      { "type": "Feature", "properties": { "module": "TGR01007", "tlid": 2 }, "geometry": { "type": "Point", "coordinates": [-87.067872, 33.093221] }},
                      { "type": "Feature", "properties": { "module": "TGR01007", "tlid": 3 }, "geometry": { "type": "Point", "coordinates": [-87.067872, 33.093221] }}
                      ]}
                   """

    "be able to read all features by only reading values on " in {
      val (_, jph) = mkTestJsonParserHelper(inputStr)

      @tailrec
      def readFeatures(jph: GeoJsonParserHelper) : Vector[Feature] = jph.currentState match {
        case ParserStates.END_FEATURES_ARRAY => jph.readFeatures
        case _ if jph.atEnd => jph.readFeatures
        case _ => readFeatures(jph.next)
      }

      val result = readFeatures(jph)
      (result must have size(3)) and (result(1).getProperty("tlid") must_== 1)  and (result(3).getProperty("tlid") must_== 3)

    }


  }


}
