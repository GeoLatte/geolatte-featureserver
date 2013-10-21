package nosql.mongodb

import scala.language.implicitConversions
import org.specs2.mutable.Specification

import play.api.libs.json._
import org.geolatte.geom._
import java.util.Date
import java.text.SimpleDateFormat
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsNumber
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve._
import nosql.json._
import nosql.json.GeometryReaders._


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 */
class FeatureTransformerSpecs extends Specification {

  import FeatureGenerator._

  val testSize = 5
  val numPointsPerLineString = 2
  val crs = CrsId.valueOf(3000)
  val maxExtent = new Envelope(0,0,1000,1000, crs)
  val indexLevel = 4
  implicit val mortonCode = new MortonCode( new MortonContext(maxExtent, indexLevel) )


  "A FeatureIndexingTranformer" should {

    val transfo = FeatureTransformers.mkFeatureIndexingTranformer(maxExtent, 4)

    "add for point features the correct _mc and _bbox properties" in {
      val pnt = generatePoint("02")
      val pf = generateFeaturesFromGeometries(List(pnt))(0)
      val json = Json.parse(pf)
      val transformResult = json.transform( transfo )
      val expectedExtent = Json.toJson[Extent](pnt.getEnvelope)
      val expectedMc = JsString(mortonCode.ofGeometry(pnt))

      def extract(r : JsResult[JsObject], p : String) = r match {
        case JsSuccess(o, _) => (o \ p).asOpt[JsValue]
        case _ => None
      }

      (transformResult must beAnInstanceOf[JsSuccess[JsObject]]) and
          ( extract(transformResult, "properties") must beSome) and
          ( extract(transformResult, SpecialMongoProperties.MC) must beSome(expectedMc)) and
          ( extract(transformResult, SpecialMongoProperties.BBOX) must beSome(expectedExtent))

    }

  }

}


object FeatureGenerator {

  import scala.collection.JavaConversions._


  def timeString = new SimpleDateFormat().format( new Date() )


  type FeatureGenerator = (Int, Geometry, JsObject) => JsObject

  type PropertiesGenerator = (Int) => JsObject


  def genFeatures(feature: FeatureGenerator, prop : PropertiesGenerator)(geometries : Seq[Geometry]) = {
    val s = geometries.foldLeft ( (List[String](), 1) ) ( (s, g) =>  (feature(s._2, g, prop(s._2)).toString() :: s._1 , s._2 + 1 ))
    s._1
  }

  val simplePropGenerator = (id: Int) => Json.obj (
    "p1" -> JsNumber(id),
    "foo" -> JsString("bar"),
    "date" -> JsString( timeString )
  )

  val simpleFeatureGenerator = (id: Int, geom: Geometry, props : JsObject) => Json.obj (
    "id" -> JsNumber(id),
    "geometry" -> Json.toJson(geom)(GeometryWithoutCrsWrites) ,
    "properties" -> props
  )

  val generateFeaturesFromGeometries = genFeatures(simpleFeatureGenerator, simplePropGenerator)_

  implicit def mortonCode2Envelope(mcVal: String)(implicit mc : MortonCode) : Envelope  = {
    mc.envelopeOf(mcVal)
  }

  def generatePointSequence(size: Int, closed : Boolean = false)(implicit extent: Envelope) =
    Range(0,size)
      .foldLeft( (Point.createEmpty(), PointSequenceBuilders.fixedSized(size, DimensionalFlag.d2D, extent.getCrsId)) )(
      (state, i) => {
        val (startPnt, ps) = state
        val x = extent.getMinX + Math.random()*extent.getWidth
        val y = extent.getMinY + Math.random()*extent.getHeight
        if (i == 0) (Points.create2D(x,y, extent.getCrsId), ps.add(x,y))
        else if (i == size && closed) (startPnt, ps.add(startPnt.getX, startPnt.getY))
        else (startPnt, ps.add(x,y))
      }
    )._2.toPointSequence


  def generatePoint(implicit extent: Envelope) = generatePointSequence(1).toList(0)


  def generateLineString(lsSize: Int)(implicit extent: Envelope) =
    new LineString(generatePointSequence(lsSize))


  def generateMultiLineString(mlsSize: Int, lsSize: Int)(implicit extent: Envelope) =
        new MultiLineString( ( for( i <- Range(0, mlsSize)) yield generateLineString(lsSize) ).toArray)

}