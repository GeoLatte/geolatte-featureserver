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

  def generateFeature = """{"properties":{"foo":3, "bar": {"l3" : 3}}, "geometry": {"type": "LineString", "coordinates": [[1,2], [3,4]]}}"""

  def genFeatures(n: Int) = (for (i <- 0 until n) yield generateFeature).fold("")((s, f) => s ++ f ++ ",").dropRight(1)


  def testEnumerator(size: Int, batchSize: Int = 64) = {
    val text = """{"crs": 31370, "features":[""" ++ genFeatures(size) ++ "]}"
    val batched = text.toCharArray.grouped(batchSize)
    (text, Enumerator.enumerate(batched))
  }

  def collectFeatures(builder: VectorBuilder[Feature]) : Iteratee[Option[Feature], State] = {
    val el = Enumeratee.mapInput[Option[Feature]] ( of => of match {
      case Input.El(Some(f)) =>
         builder += f
        Input.Empty
      case Input.El(None) => Input.El(State("Import Error"))
      case _ => Input.Empty
    })
    // at the end discard the state messages but substitute the result
    el &>> Iteratee.fold( State() )( stateFolder)
  }

  def isValidFeatureList[A](result : List[A])  = {
    if ( result.filterNot(s => s match {
      case f:Feature if f.getGeometry.getCrsId.equals(CrsId.valueOf(31370))  => true
      case _ => false
    }).size == 0) ok(": list of features")
    else ko("list of features")
  }

//  def test[B](inp: Enumerator[Array[Char]], enumeratee: Enumeratee[Array[Char], B]) = {
//    val future = Iteratee.flatten(inp |>> ( enumeratee &>> Iteratee.getChunks)).run
//    Await.result(future, Duration(5000, "millis"))
//  }

  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {
      val testSize = 15
      val (text, enumerator) = testEnumerator(testSize)
      val sink = new VectorBuilder[Feature]
      val future = Iteratee.flatten(enumerator |>> (featureCollectionEnumeratee(collectFeatures(sink)) &>> Iteratee.getChunks)).run
      val states = Await.result(future, Duration(5000, "millis"))
      val result = sink.result().toList

      (result must not be empty) and
        (result must beLike {
          case l: List[_] if l.size == testSize => isValidFeatureList(l)
          case _ => ko(": second result element is not of correct size")
        })

    }
  }

}
