package org.geolatte.nosql.json

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/31/13
 */

import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.geolatte.geom._
import org.geolatte.common.Feature
import java.util
import scala.collection.DefaultMap
import org.geolatte.geom.crs.CrsId


object GeometryReaders {

  trait Positions

  object EmptyPosition extends Positions

  case class Position(pnt: Point) extends Positions {
    def this(x: Double, y: Double)(implicit crs: CrsId) = this(Points.create2D(x, y, crs))
  }

  case class PositionList(list: Seq[Positions]) extends Positions

  def positionList2PointSequence(seq: Seq[Positions]): Option[PointSequence] = {
    val seqPnts = seq.map {
      case Position(pnt) => pnt
    }
    seqPnts match {
      case Seq() => Some(EmptyPointSequence.INSTANCE)
      case _ => {
        val fp = seqPnts.head
        val builder = PointSequenceBuilders.variableSized(fp.getDimensionalFlag, fp.getCrsId)
        val ps = seqPnts.foldLeft(builder)((b, p) => b.add(p)).toPointSequence
        Some(ps)
      }
    }
  }

  def positions2PointSequence(pos: Positions): Option[PointSequence] = {
    import PointSequenceBuilders._
    pos match {
      case EmptyPosition => Some(EmptyPointSequence.INSTANCE)
      case Position(pnt) => Some(fixedSized(1, pnt.getDimensionalFlag, pnt.getCrsId).add(pnt).toPointSequence)
      case PositionList(l) => positionList2PointSequence(l)
    }
  }

  def PositionReads(implicit crs: CrsId): Reads[Positions] = new Reads[Positions] {

    def readArray[T](values: Seq[T]): Positions =
      values match {
        case Nil => EmptyPosition
        case Seq((x: JsNumber), (y: JsNumber)) => new Position(x.value.doubleValue(), y.value.doubleValue())
        case psv: Seq[_] => {
          val pss = for (v <- psv) yield v match {
            case JsArray(a) => readArray(a)
            case _ => throw new IllegalStateException() // improve error-handling
          }
          PositionList(pss)
        }
      }

    def reads(json: JsValue): JsResult[Positions] = json match {
      case JsArray(arr) => JsSuccess(readArray(arr))
      case _ => JsError("No array for coordinates property")
    }
  }

  def GeometryReads(implicit crs: CrsId) = new Reads[Geometry] {

    def toGeometry(typeKey: String, pos: Positions): Geometry = (typeKey.toLowerCase, pos) match {
        case ("point", Position(pnt)) => pnt
        case ("linestring", PositionList(list)) => new LineString(positionList2PointSequence(list).get)
        case ("multilinestring", PositionList(list)) => {
          val linestrings : Array[LineString] = list.map( l => toGeometry("linestring", l).asInstanceOf[LineString]).toArray
          new MultiLineString(linestrings)
        }
        case _ => throw new UnsupportedOperationException()

      }

    def reads(json: JsValue): JsResult[Geometry] = try {
      JsSuccess(toGeometry(
        (json \ "type").as[String],
        (json \ "coordinates").as(PositionReads)
      ))

    } catch {
      case ex: Throwable => JsError(ex.getMessage)
    }
  }

  //don't make this implicit because it might match inadvertently
  val anyReads = new Reads[AnyRef] {

    def reads(json: JsValue): JsResult[AnyRef] = try {
      JsSuccess(readValue(json))
    } catch {
      case ex: Throwable => JsError(ex.getMessage)
    }

    def readValue(json: JsValue): AnyRef = json match {
      case JsBoolean(b) => b: java.lang.Boolean
      case JsNumber(n) => n.toLong: java.lang.Long
      case JsString(s) => s
      case JsArray(l) => l.map(readValue)
      case JsObject(o) => {
        val kvSeq = o.map {
          case (k, v) => k -> readValue(v)
        }
        Map(kvSeq: _*)
      }
      case JsNull => null
      case JsUndefined(error) => throw new RuntimeException(error)
    }

  }

  case class Properties(inner: JsObject) extends DefaultMap[String, AnyRef] {

    def get(key: String): Option[AnyRef] = (inner \ key).asOpt(anyReads)

    def iterator: Iterator[(String, AnyRef)] = inner.fields.iterator.map {
      case (key, jsv) => (key, jsv.as(anyReads))
    }

    def getKeys = inner.keys

  }

  case class JsFeature(id: Option[AnyRef], geometry: Geometry, properties: Properties) extends Feature {

    import scala.collection.JavaConversions._

    def hasProperty(propertyName: String, trueForSpecialProperties: Boolean): Boolean =
      properties.getKeys.contains(propertyName) || List("geometry", "id").contains(propertyName)

    def getProperties: util.Collection[String] = properties.getKeys

    def getProperty(propertyName: String): AnyRef = properties.get(propertyName).get

    def getId = id match {
      case Some(v) => v;
      case _ => Integer.valueOf(-1)
    }

    def getGeometry: Geometry = geometry

    def getGeometryName: String = "geometry"

    def hasId: Boolean = false

    def hasGeometry: Boolean = true
  }

  implicit val keyValueReads = new Reads[Properties] {
    def reads(json: JsValue): JsResult[Properties] = json match {
      case obj: JsObject => JsSuccess(Properties(obj))
      case _ => JsError("expected object")
    }
  }

  def FeatureReads(implicit crs: CrsId): Reads[Feature] = (
    (__ \ "id").readNullable(anyReads) and
      (__ \ "geometry").read(GeometryReads) and
      (__ \ "properties").read[Properties]
    )(JsFeature)
}

