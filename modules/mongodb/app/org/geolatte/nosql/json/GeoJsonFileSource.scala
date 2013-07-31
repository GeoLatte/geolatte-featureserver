package org.geolatte.nosql.json

import org.geolatte.common.dataformats.json.jackson.JsonMapper
import org.geolatte.common.Feature
import org.geolatte.nosql.Source

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 3/9/13
 */

object GeoJsonFileSource{

  private val jsonMapper = new JsonMapper

  private def jsonReader(line: String): Feature = {
    jsonMapper.fromJson(line, classOf[Feature])
  }

  private def isFeatureStr(line: String): Boolean = {
    line.contains("\"type\": \"Feature\"")
  }

  def fromFile(fName: String): Source[Feature] = {
    new Source[Feature] {
      def out() = {
        val lines = scala.io.Source.fromFile(fName).getLines()
        (lines filter isFeatureStr) map jsonReader
      }
    }
  }

}

