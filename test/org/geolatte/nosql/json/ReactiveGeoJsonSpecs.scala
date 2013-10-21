package org.geolatte.nosql.json

import org.specs2.mutable.Specification
import play.api.libs.iteratee._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.geolatte.nosql.json.ReactiveGeoJson._
import config.ConfigurationValues
import play.api.libs.json.JsObject

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
class ReactiveGeoJsonSpecs extends Specification {


  import scala.language.reflectiveCalls

  def generateFeature = """{"type" : "Feature", "properties":{"foo":3}, "geometry": {"type": "LineString", "coordinates": [[1,2], [3,4]]}}"""

  def genFeatures(n: Int) = (for (i <- 0 until n) yield generateFeature).fold("")((s, f) => s ++ f ++ ConfigurationValues.jsonSeparator).dropRight(1)


  def testEnumerator(size: Int, batchSize: Int = 64) = {
    val text = genFeatures(size)
    val batched = text.getBytes("UTF-8").grouped(batchSize).toList
    (text, Enumerator(batched:_*))
  }

  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {

      val testSize = 500
      val (_, enumerator) = testEnumerator(testSize)
      var sink = new scala.collection.mutable.ArrayBuffer[JsObject]()
      val fw = new FeatureWriter {
        def add(objects: Seq[JsObject]) = {sink ++= objects }

        def updateIndex() {}
      }
      val future = (enumerator  andThen Enumerator.eof) |>>> mkStreamingIteratee(fw)
      val stateIteratee = Await.result(future, Duration(5000, "millis"))
      val result = sink.toList

      (stateIteratee must beRight) and
      (result must not be empty) and
        (result must beLike {
          case l: List[_]  if l.size == testSize => ok
          case _ => ko(": second result element is not of correct size")
        })

    }
  }

}
