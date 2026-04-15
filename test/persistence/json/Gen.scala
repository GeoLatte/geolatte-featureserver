package persistence.json

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.geolatte.geom._
import org.geolatte.geom.crs.{ CoordinateReferenceSystem, CoordinateReferenceSystems }
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
 *
 * Migrated to geolatte-geom 1.11. Test fixtures use the existential CRS pattern:
 * the test owner picks an Envelope (with its CRS), then generators capture P
 * from that CRS for every constructed geometry.
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

  /** Test "dimensional flag" sentinel — replaces the removed `DimensionalFlag` enum. */
  sealed trait Dim {
    def hasZ: Boolean
    def hasM: Boolean
    def coordCount: Int = 2 + (if (hasZ) 1 else 0) + (if (hasM) 1 else 0)
  }
  case object D2D  extends Dim { val hasZ = false; val hasM = false }
  case object D3D  extends Dim { val hasZ = true;  val hasM = false }
  case object D2DM extends Dim { val hasZ = false; val hasM = true  }
  case object D3DM extends Dim { val hasZ = true;  val hasM = true  }

  // Backwards-compatible aliases for the old `DimensionalFlag._` import sites.
  val d2D  = D2D
  val d3D  = D3D
  val d2DM = D2DM
  val d3DM = D3DM

  def gen[T](inner: () => Option[T]): Gen[T] = new Gen[T] { def sample = inner() }

  def apply[T](t: T): Gen[T] = new Gen[T] { def sample = Some(t) }

  def fail[T](): Gen[T] = new Gen[T] { def sample = None }

  def id: Gen[Int] = gen {
    { val i: AtomicInteger = new AtomicInteger(1); () => Some(i.getAndIncrement) }
  }

  def idString: Gen[String] = gen { () => Some(UUID.randomUUID().toString) }

  def sequence[T](list: List[Gen[T]]): Gen[List[T]] =
    new Gen[List[T]] {
      def sample = {
        val reversed = list.foldLeft[Option[List[T]]](Some(List[T]()))(
          (lOpt, e) => e.sample.flatMap(v => lOpt.map(l => v :: l))
        )
        reversed.map(l => l.reverse)
      }
    }

  def listOf[T](size: Int, g: Gen[T]): Gen[List[T]] = sequence(List.fill(size)(g))

  def oneOf[T](elems: T*): Gen[T] = new Gen[T] {
    def sample: Option[T] = Some(elems(Random.nextInt(elems.size)))
  }

  // ---------------------------------------------------------------------------
  // Geometry generators — capture P once per call from the implicit Envelope.
  // ---------------------------------------------------------------------------

  /**
   * Promote the test envelope's base 2D CRS to the dimensional variant required
   * by the test.
   */
  private def promoteCrs(extent: Envelope[_], dim: Dim): CoordinateReferenceSystem[_] =
    CoordinateReferenceSystems.adjustTo(
      extent.getCoordinateReferenceSystem,
      dim.hasZ,
      dim.hasM
    )

  private def randomXY(extent: Envelope[_]): (Double, Double) = {
    val a = extent.toArray() // [xmin, ymin, xmax, ymax]
    val width  = a(2) - a(0)
    val height = a(3) - a(1)
    (a(0) + Math.random() * width, a(1) + Math.random() * height)
  }

  private def randomCoords(extent: Envelope[_], dim: Dim): Array[Double] = {
    val (x, y) = randomXY(extent)
    val z = 1 + 100 * Math.random()
    val m = 1 + 100 * Math.random()
    dim match {
      case D2D  => Array(x, y)
      case D3D  => Array(x, y, z)
      case D2DM => Array(x, y, m)
      case D3DM => Array(x, y, z, m)
    }
  }

  /**
   * Build a PositionSequence on the dim-promoted CRS captured from `extent`.
   */
  def positionSequence(size: Int, dim: Dim = D2D, closed: Boolean = false)(
      implicit extent: Envelope[_]
  ): Gen[PositionSequence[_]] = new Gen[PositionSequence[_]] {
    def sample: Option[PositionSequence[_]] = {
      val crs = promoteCrs(extent, dim)
      Some(withCapturedCrs(crs)(new CrsHandler[PositionSequence[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): PositionSequence[_] = {
          val builder = PositionSequenceBuilders.fixedSized(size, c.getPositionClass)
          def add(): Unit = builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
          if (!closed) {
            (0 until size).foreach(_ => add())
          } else {
            val startCoords = randomCoords(extent, dim)
            builder.add(Positions.mkPosition(c, startCoords: _*))
            (1 until size - 1).foreach(_ => add())
            builder.add(Positions.mkPosition(c, startCoords: _*))
          }
          builder.toPositionSequence
        }
      }))
    }
  }

  // Backwards-compatible alias for the old name used in tests.
  def pointSequence(size: Int, dim: Dim = D2D, closed: Boolean = false)(
      implicit extent: Envelope[_]
  ): Gen[PositionSequence[_]] = positionSequence(size, dim, closed)

  implicit def toFieldWrappingGen[T](g: Gen[T])(implicit w: Writes[T]): Gen[JsValueWrapper] =
    g.map(v => Json.toJsFieldJsValueWrapper(v))

  def properties(pairs: (String, Gen[JsValueWrapper])*): Gen[JsObject] = {
    val kvMap = pairs.toMap
    sequence(kvMap.values.toList).map(gL => Json.obj(kvMap.keys.toList.zip(gL): _*))
  }

  def point(dim: Dim = D2D)(implicit extent: Envelope[_]): Gen[Point[_]] =
    new Gen[Point[_]] {
      def sample: Option[Point[_]] = {
        val crs = promoteCrs(extent, dim)
        Some(withCapturedCrs(crs)(new CrsHandler[Point[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): Point[_] =
            Geometries.mkPoint(Positions.mkPosition(c, randomCoords(extent, dim): _*), c)
        }))
      }
    }

  def lineString(size: Int, dim: Dim = D2D)(implicit extent: Envelope[_]): Gen[LineString[_]] =
    new Gen[LineString[_]] {
      def sample: Option[LineString[_]] = {
        val crs = promoteCrs(extent, dim)
        Some(withCapturedCrs(crs)(new CrsHandler[LineString[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): LineString[_] = {
            val builder = PositionSequenceBuilders.fixedSized(size, c.getPositionClass)
            (0 until size).foreach(_ =>
              builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
            )
            Geometries.mkLineString(builder.toPositionSequence, c)
          }
        }))
      }
    }

  def multiLineString(numLines: Int, numPoints: Int, dim: Dim = D2D)(
      implicit extent: Envelope[_]
  ): Gen[MultiLineString[_]] = new Gen[MultiLineString[_]] {
    def sample: Option[MultiLineString[_]] = {
      val crs = promoteCrs(extent, dim)
      Some(withCapturedCrs(crs)(new CrsHandler[MultiLineString[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): MultiLineString[_] = {
          val lines = (0 until numLines).map { _ =>
            val builder = PositionSequenceBuilders.fixedSized(numPoints, c.getPositionClass)
            (0 until numPoints).foreach(_ =>
              builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
            )
            Geometries.mkLineString(builder.toPositionSequence, c)
          }
          Geometries.mkMultiLineString(lines.toList: _*)
        }
      }))
    }
  }

  def linearRing(size: Int, dim: Dim = D2D)(implicit extent: Envelope[_]): Gen[LinearRing[_]] =
    new Gen[LinearRing[_]] {
      def sample: Option[LinearRing[_]] = {
        val crs = promoteCrs(extent, dim)
        Some(withCapturedCrs(crs)(new CrsHandler[LinearRing[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): LinearRing[_] = {
            val builder = PositionSequenceBuilders.fixedSized(size, c.getPositionClass)
            val startCoords = randomCoords(extent, dim)
            builder.add(Positions.mkPosition(c, startCoords: _*))
            (1 until size - 1).foreach(_ =>
              builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
            )
            builder.add(Positions.mkPosition(c, startCoords: _*))
            Geometries.mkLinearRing(builder.toPositionSequence, c)
          }
        }))
      }
    }

  def polygon(numPoints: Int, dim: Dim = D2D)(implicit extent: Envelope[_]): Gen[Polygon[_]] =
    new Gen[Polygon[_]] {
      def sample: Option[Polygon[_]] = {
        val crs = promoteCrs(extent, dim)
        Some(withCapturedCrs(crs)(new CrsHandler[Polygon[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): Polygon[_] = {
            val builder = PositionSequenceBuilders.fixedSized(numPoints, c.getPositionClass)
            val startCoords = randomCoords(extent, dim)
            builder.add(Positions.mkPosition(c, startCoords: _*))
            (1 until numPoints - 1).foreach(_ =>
              builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
            )
            builder.add(Positions.mkPosition(c, startCoords: _*))
            val ring = Geometries.mkLinearRing(builder.toPositionSequence, c)
            Geometries.mkPolygon(ring)
          }
        }))
      }
    }

  def multiPolygon(numPoly: Int, numPoints: Int, dim: Dim = D2D)(
      implicit extent: Envelope[_]
  ): Gen[MultiPolygon[_]] = new Gen[MultiPolygon[_]] {
    def sample: Option[MultiPolygon[_]] = {
      val crs = promoteCrs(extent, dim)
      Some(withCapturedCrs(crs)(new CrsHandler[MultiPolygon[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): MultiPolygon[_] = {
          val polys = (0 until numPoly).map { _ =>
            val builder = PositionSequenceBuilders.fixedSized(numPoints, c.getPositionClass)
            val startCoords = randomCoords(extent, dim)
            builder.add(Positions.mkPosition(c, startCoords: _*))
            (1 until numPoints - 1).foreach(_ =>
              builder.add(Positions.mkPosition(c, randomCoords(extent, dim): _*))
            )
            builder.add(Positions.mkPosition(c, startCoords: _*))
            val ring = Geometries.mkLinearRing(builder.toPositionSequence, c)
            Geometries.mkPolygon(ring)
          }
          Geometries.mkMultiPolygon(polys.toList: _*)
        }
      }))
    }
  }

  def geometryCollection(size: Int, dim: Dim = D2D)(
      implicit extent: Envelope[_]
  ): Gen[GeometryCollection[_]] = new Gen[GeometryCollection[_]] {
    def sample: Option[GeometryCollection[_]] = {
      val crs = promoteCrs(extent, dim)
      Some(withCapturedCrs(crs)(new CrsHandler[GeometryCollection[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): GeometryCollection[_] = {
          val pts = (0 until size).map(_ =>
            Geometries.mkPoint(Positions.mkPosition(c, randomCoords(extent, dim): _*), c)
          )
          Geometries.mkGeometryCollection(pts.toList: _*)
        }
      }))
    }
  }

  def geoJsonFeature[T: ClassTag](id: Gen[T], geom: Gen[Geometry[_]], prop: Gen[JsObject]): Gen[JsObject] =
    for {
      g <- geom
      p <- prop
      i <- id
    } yield {
      val gJson = geometryWrites.writes(g)
      i match {
        case i: String => Json.obj("type" -> "Feature", "id" -> i, "geometry" -> gJson, "properties" -> p)
        case i: Int    => Json.obj("type" -> "Feature", "id" -> i.asInstanceOf[Int], "geometry" -> gJson, "properties" -> p)
        case _         => Json.obj("type" -> "Feature", "id" -> i.toString, "geometry" -> gJson, "properties" -> p)
      }
    }

  def geoJsonFeatureArray(jsonGen: Gen[JsObject], size: Int): Gen[JsArray] =
    sequence(List.fill(size)(jsonGen)).map(js => JsArray(js))

  implicit def mortonCode2Envelope(mcVal: String)(implicit mc: MortonCode[_]): Envelope[_] =
    mc.envelopeOf(mcVal)
}
