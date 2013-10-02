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
import play.api.data.validation.ValidationError
import scala.util.{Success, Try}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer


object GeometryReaders {

  trait Positions

  object EmptyPosition extends Positions

  case class InvalidPosition(msg: String) extends Positions

  case class Position(x: Double, y: Double) extends Positions

  case class PositionList(list: ListBuffer[Positions]) extends Positions


  def positionList2PointSequence(seq: Seq[Positions])(implicit crs: CrsId): Option[PointSequence] = {
    val seqPnts = seq.map {
      case Position(x, y) => Points.create2D(x, y, crs)
      case InvalidPosition(msg) => throw new IllegalStateException(msg)
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

  implicit val PositionReads: Reads[Positions] = new Reads[Positions] {

    def readArray[T](values: Seq[T]): Positions = {

      def toPos(el: Any) = el match {
        case JsArray(a) => readArray(a)
        case _ => InvalidPosition("Irregular structure in coordinates array")
      }

      values match {
        case Nil => EmptyPosition
        case Seq((x: JsNumber), (y: JsNumber)) => Position(x.value.doubleValue(), y.value.doubleValue())
        case psv: Seq[_] => {
          val pl = psv.foldLeft(ListBuffer[Positions]())( (result, el) => result :+ toPos(el) )
          PositionList(pl)
        }
      }
    }

    def reads(json: JsValue): JsResult[Positions] = json match {
      case JsArray(arr) => JsSuccess(readArray(arr))
      case _ => JsError("No array for coordinates property")
    }

  }

  implicit val crsIdReads = (__ \ "properties" \ "name").read[String].map(epsg => Try(CrsId.parse(epsg))).collect(ValidationError("Exception on parsing of EPSG text")) {
    case Success(crs) => crs
  }

  def GeometryReads(implicit defaultCrs: CrsId) = new Reads[Geometry] {

    def mkLineString(list: Seq[Positions]): LineString = new LineString(positionList2PointSequence(list).get)

    def toGeometry(typeKey: String, pos: Positions, geomcrs: Option[CrsId]): Geometry = {
      val crs = geomcrs.getOrElse(defaultCrs)
      (typeKey.toLowerCase, pos) match {
        case ("point", Position(x, y)) => Points.create2D(x, y, crs)
        case ("linestring", PositionList(list)) => mkLineString(list)
        case ("multilinestring", PositionList(list)) => {
          val linestrings: Array[LineString] = list.collect {
            case PositionList(l) => mkLineString(l)
          }.toArray
          new MultiLineString(linestrings)
        }
        case _ => throw new UnsupportedOperationException()
      }
    }

    def reads(json: JsValue): JsResult[Geometry] = try {
      JsSuccess(toGeometry(
        (json \ "type").as[String],
        (json \ "coordinates").as(PositionReads),
        (json \ "crs").asOpt(crsIdReads)
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
      case JsNumber(n) => n
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
      case Some(v) => v
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

