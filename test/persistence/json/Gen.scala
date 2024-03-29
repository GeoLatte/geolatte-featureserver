package persistence.json

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.geolatte.geom._
import org.geolatte.geom.curve.MortonCode
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._
import persistence.GeoJsonFormats._

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

//
// TODO -- Make this compatible with ScalaCheck
//

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/15/13
 */
trait Gen[+T] {

  import Gen._

  def sample: Option[T]

  def flatMap[U](f: (T) => Gen[U]): Gen[U] = gen[U](() => sample match {
    case Some(t) =>
      try { f(t).sample } catch { case _: Throwable => None }
    case _ => fail().sample
  })

  def map[U](f: (T) => U): Gen[U] = gen(() => sample match {
    case Some(t) =>
      try { Some(f(t)) } catch { case _: Throwable => None }
    case None => None
  })

  def filter(f: (T) => Boolean): Gen[T] = gen(() => sample match {
    case Some(t) if f(t) => Some(t)
    case _               => None
  })

}

object Gen {

  import DimensionalFlag._

  def gen[T](inner: () => Option[T]): Gen[T] = new Gen[T] {
    def sample = inner()
  }

  def apply[T](t: T): Gen[T] = new Gen[T] {
    def sample = Some(t)
  }

  def fail[T](): Gen[T] = new Gen[T] {
    def sample = None
  }

  def id: Gen[Int] = gen {
    { val i: AtomicInteger = new AtomicInteger(1); () => Some(i.getAndIncrement) }
  }

  def idString: Gen[String] = gen {
    { () => Some(UUID.randomUUID().toString) }
  }

  /**
   * Turns a list of Gen[T] into a Gen[List[T]]
   * if one of the generators fails (returns None), the result Gen will fail also
   */
  def sequence[T](list: List[Gen[T]]): Gen[List[T]] =
    new Gen[List[T]] {
      def sample = {
        val reversed = list.foldLeft[Option[List[T]]](Some(List[T]()))((lOpt, e) => e.sample.flatMap(v => lOpt.map(l => v :: l)))
        reversed.map(l => l.reverse)
      }
    }

  /*
  Generates a generator for a list of size n, provided the generator does not fail.
   */
  def listOf[T](size: Int, g: Gen[T]): Gen[List[T]] = sequence(List.fill(size)(g))

  def oneOf[T](elems: T*): Gen[T] = new Gen[T] {
    def sample: Option[T] = Some(elems(Random.nextInt(elems.size)))
  }

  def pointSequence(size: Int, dimFlag: DimensionalFlag = d2D, closed: Boolean = false)(implicit extent: Envelope): Gen[PointSequence] = new Gen[PointSequence] {

    def createPnt = {
      val x = extent.getMinX + Math.random() * extent.getWidth
      val y = extent.getMinY + Math.random() * extent.getHeight
      val z = 1 + 100 * Math.random()
      val m = 1 + 100 * Math.random()
      dimFlag match {
        case DimensionalFlag.d2D  => Points.create2D(x, y, extent.getCrsId)
        case DimensionalFlag.d3D  => Points.create3D(x, y, z, extent.getCrsId)
        case DimensionalFlag.d2DM => Points.create2DM(x, y, m, extent.getCrsId)
        case DimensionalFlag.d3DM => Points.create3DM(x, y, z, m, extent.getCrsId)
      }
    }

    def sample = {
      val builder = PointSequenceBuilders.fixedSized(size, dimFlag, extent.getCrsId)
      if (!closed) {

        Range(0, size).foreach(_ => builder.add(createPnt))

      } else {
        val startPnt = createPnt
        builder.add(startPnt)
        Range(1, size - 1).foreach(_ => builder.add(createPnt))
        builder.add(startPnt)
      }
      Some(builder.toPointSequence)
    }
  }

  implicit def toFieldWrappingGen[T](g: Gen[T])(implicit w: Writes[T]): Gen[JsValueWrapper] = g.map(v => Json.toJsFieldJsValueWrapper(v))

  def properties(pairs: (String, Gen[JsValueWrapper])*): Gen[JsObject] = {
    val kvMap = pairs.toMap
    sequence(kvMap.values.toList).map(gL => Json.obj(kvMap.keys.toList.zip(gL): _*))
  }

  def point(dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) = pointSequence(1, dimFlag).map(ps => new Point(ps))

  def lineString(size: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) =
    pointSequence(size, dimFlag).map(ps => new LineString(ps))

  def multiLineString(numLines: Int, numPoints: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) = {
    val sizedList: List[Int] = List.fill(numLines)(0)
    val genListOfLineStrings = sequence(sizedList.map(_ => lineString(numPoints, dimFlag)))
    genListOfLineStrings.map(ls => new MultiLineString(ls.toArray))
  }

  def linearRing(size: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) =
    pointSequence(size, dimFlag, closed = true).map(ps => new LinearRing(ps))

  def polygon(numPoints: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) = {
    linearRing(numPoints, dimFlag).map(lr => new Polygon(Array(lr)))
  }

  def multiPolygon(numPoly: Int, numPoints: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope) = {
    val sizedList: List[Int] = List.fill(numPoly)(0)
    val genListOfPolys = sequence(sizedList.map(_ => polygon(numPoints, dimFlag)))
    genListOfPolys.map(lp => new MultiPolygon(lp.toArray))
  }

  def geometryCollection(size: Int, dimFlag: DimensionalFlag = d2D)(implicit extent: Envelope): Gen[GeometryCollection] = {
    val sizedList: List[Int] = List.fill(size)(0)
    val genListOf = sequence(sizedList.map(_ => point(dimFlag)))
    genListOf.map(g => new GeometryCollection(g.toArray))
  }

  def geoJsonFeature[T: ClassTag](id: Gen[T], geom: Gen[Geometry], prop: Gen[JsObject]): Gen[JsObject] =
    for {
      g <- geom
      p <- prop
      i <- id
    } yield {
      i match {
        case i: String => Json.obj("type" -> "Feature", "id" -> i, "geometry" -> Json.toJson(g).as[Geometry], "properties" -> p)
        case i: Int    => Json.obj("type" -> "Feature", "id" -> i.asInstanceOf[Int], "geometry" -> Json.toJson(g).as[Geometry], "properties" -> p)
        case _         => Json.obj("type" -> "Feature", "id" -> i.toString, "geometry" -> Json.toJson(g).as[Geometry], "properties" -> p)
      }
    }

  def geoJsonFeatureArray(jsonGen: Gen[JsObject], size: Int): Gen[JsArray] = sequence(List.fill(size)(jsonGen)).map(js => JsArray(js))

  implicit def mortonCode2Envelope(mcVal: String)(implicit mc: MortonCode): Envelope = {
    mc.envelopeOf(mcVal)
  }

}
