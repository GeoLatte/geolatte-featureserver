package nosql.mongodb

import org.specs2.mutable.Specification
import play.api.libs.iteratee._
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import config.ConfigurationValues
import play.api.libs.json.JsObject
import nosql.json.Gen
import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
class ReactiveGeoJsonSpecs extends Specification {


  import scala.language.reflectiveCalls
  import scala.concurrent.ExecutionContext.Implicits.global

  def genFeatures(size: Int) = {
    implicit val extent = new Envelope(0,0,90,90)
    val fGen = Gen.geoJsonFeature(Gen.id, Gen.lineString(3), Gen.properties( "foo" -> Gen.oneOf("boo", "bar")))
    for(i <- 0 until size) yield fGen.sample
  }

  def testEnumerator(size: Int, batchSize: Int = 1000) = {
    val text = genFeatures(size).map( g => g.get) mkString ConfigurationValues.jsonSeparator
    val batched = text.getBytes("UTF-8").grouped(batchSize).toList
    (text, Enumerator(batched:_*))
  }

  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {

      val testSize = 500
      val (_, enumerator) = testEnumerator(testSize)
      var sink = new scala.collection.mutable.ArrayBuffer[JsObject]()
      val fw = new FeatureWriter {
        def add(objects: Seq[JsObject]) = { sink ++= objects ; Future {objects.size} }
        def updateIndex() = Future.successful(true)
      }

      val future = (enumerator  andThen Enumerator.eof) |>>> ReactiveGeoJson.mkStreamingIteratee(fw)
      //Wait until de iteratee is done
      val stateIteratee = Await.result(future, Duration(5000, "millis"))
      //Wait until de Iteratee is finished writing to featurewriter
      Await.result(stateIteratee.right.get, Duration(5000, "millis"))
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
