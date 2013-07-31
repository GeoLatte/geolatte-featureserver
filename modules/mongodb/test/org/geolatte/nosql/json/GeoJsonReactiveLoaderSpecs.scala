package org.geolatte.nosql.json

import org.specs2.mutable.Specification
import play.api.libs.iteratee._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.geolatte.nosql.json.ReactiveGeoJson._
import org.geolatte.common.Feature

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
class GeoJsonReactiveLoaderSpecs extends Specification {

  import scala.concurrent.ExecutionContext.Implicits.global

  def generateFeature = """{"properties":{"foo":3, "bar": {"l3" : 3}}, "geometry": {"type": "LineString", "coordinates": [[1,2], [3,4]]}}"""

  def genFeatures(n: Int) = (for (i <- 0 until n) yield generateFeature).fold("")((s, f) => s ++ f ++ ",").dropRight(1)


  def testEnumerator(size: Int, batchSize: Int = 64): Enumerator[Array[Char]] = {
    val text = """{"crs": 31370, "features":[""" ++ genFeatures(size) ++ "]}"
    val batched = text.toCharArray.grouped(batchSize)
    Enumerator.enumerate(batched)
  }

  def isValidFeatureList[A](result : List[A])  = {
    if ( result.filterNot(s => s match {
      case Some(f:Feature) => true
      case _ => false
    }).size == 0) ok(": list of features")
    else ko("list of features")
  }

  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {
      val enumerator = testEnumerator(10)
      val future = Iteratee.flatten(enumerator |>> (featureCollectionIteratee(Iteratee.getChunks) &>> Iteratee.getChunks)).run
      val result = Await.result(future, Duration(5000, "millis"))

      (result must not be empty) and
        (result must have size (2)) and
        (result(0) must_== 31370) and
        (result(1) must beLike {
          case l: List[_] if l.size == 10 => isValidFeatureList(l)
          case _ => ko(": second result element is not a list")
        })

    }
  }

}
