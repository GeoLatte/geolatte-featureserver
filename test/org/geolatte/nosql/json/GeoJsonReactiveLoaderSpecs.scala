package org.geolatte.nosql.json

import org.specs2.mutable.Specification
import play.api.libs.iteratee._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.geolatte.nosql.json.ReactiveGeoJson._
import org.geolatte.common.Feature
import org.geolatte.geom.crs.CrsId
import scala.collection.immutable.VectorBuilder

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
class GeoJsonReactiveLoaderSpecs extends Specification {


  import scala.language.reflectiveCalls
  import scala.concurrent.ExecutionContext.Implicits.global

  def generateFeature = """{"type" : "Feature", "properties":{"foo":3, "bar": {"l3" : 3}}, "geometry": {"type": "LineString", "coordinates": [[1,2], [3,4]]}}"""

  def genFeatures(n: Int) = (for (i <- 0 until n) yield generateFeature).fold("")((s, f) => s ++ f ++ ",").dropRight(1)


  def testEnumerator(size: Int, batchSize: Int = 64) = {
    val text = """{"crs": 31370, "features":[""" ++ genFeatures(size) ++ "]}"
    val batched = text.getBytes("UTF-8").grouped(batchSize)
    (text, Enumerator.enumerate(batched))
  }


  def isValidFeatureList[A](result : List[A])  = {
    if ( result.filterNot(s => s match {
      case f:Feature if f.getGeometry.getCrsId.equals(CrsId.valueOf(31370))  => true
      case _ => false
    }).size == 0) ok(": list of features")
    else ko("list of features")
  }


  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {
      val testSize = 15
      val (text, enumerator) = testEnumerator(testSize)
      var sink = new scala.collection.mutable.ArrayBuffer[Feature]()
      val fw = new FeatureWriter {
        def add(f: Feature): Boolean = {sink += f; true}
        def flush() {}
      }
      val future = Iteratee.flatten(enumerator |>> mkStreamingIteratee(fw) ).run
      val states = Await.result(future, Duration(5000, "millis"))
      println(states)
      val result = sink.toList

      (result must not be empty) and
        (result must beLike {
          case l: List[_] if l.size == testSize => isValidFeatureList(l)
          case _ => ko(": second result element is not of correct size")
        })

    }
  }

}
