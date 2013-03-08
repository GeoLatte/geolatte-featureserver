package org.geolatte.nosql

import org.geolatte.common._
import dataformats.json.jackson.JsonMapper
import java.io.{IOException, FileNotFoundException}
import org.geolatte.geom.Envelope

trait Sink {
  def consume(iterator: Iterator[Feature]): Unit
}

trait Source{
  def produce(): Iterator[Feature]
}

/**
 * A stackable modification trait that can be mixed in
 * into a Source so that it allows for window querying
 *
 * (See ProgInScala, section 12.5)
 *
 */
trait WindowQueryable extends Source {
  def produce(window: Envelope): Iterator[Feature]
}

object StdOutSink extends Sink {
  def consume(iterator: Iterator[Feature]) {
    iterator.foreach( (f) => println(f.getGeometry().asText()) )
  }
}
object GeoJSONFileSourceFactory {

 private val jsonMapper = new JsonMapper

 private def jsonReader(line: String) : Feature = {
    jsonMapper.fromJson(line, classOf[Feature])
 }

 private def isFeatureStr(line: String): Boolean = {
   line.contains("\"type\": \"Feature\"")
 }

 def fromFile(fName : String): Source = {
    new Source{
      def produce() = {
        val lines  = scala.io.Source.fromFile(fName).getLines()
        (lines filter isFeatureStr) map jsonReader
      }
    }
 }

}
