package org.geolatte.nosql.json

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/31/13
 */

import scala.language.implicitConversions

import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import play.api.data.validation.ValidationError
import scala.util.{Success, Try}
import scala.collection.mutable.ListBuffer


object GeometryReaders {

  trait Extent {
    def union(other: Extent) : Extent
    def toEnvelope(crs: CrsId) : Envelope
    def toList: List[Double]
  }

  object EmptyExtent extends Extent {
    def union(other: Extent) = other
    def toEnvelope(crs: CrsId) = Envelope.EMPTY
    def toList = List[Double]()
  }
  case class NonEmptyExtent(xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Extent {

    def union(other: Extent) = other match {
      case EmptyExtent => this
      case NonEmptyExtent(x2min, y2min, x2max, y2max) => NonEmptyExtent(
        Math.min(xmin, x2min),
        Math.min(ymin, y2min),
        Math.max(xmax, x2max),
        Math.max(ymax, y2max)
      )
    }

    def toEnvelope(crs: CrsId) = new Envelope(xmin, ymin, xmax, ymax, crs)

    def toList = List(xmin, ymin, xmax, ymax)

  }

  implicit def Envelope2Extent(env : Envelope) : Extent =
        if (env.isEmpty) EmptyExtent
        else NonEmptyExtent(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

  trait Positions {
    def envelope(id: CrsId) = boundingBox.toEnvelope(id)

    def boundingBox : Extent
    def expand(ex: Extent) = boundingBox union ex
  }

  object EmptyPosition extends Positions {
    val boundingBox = EmptyExtent
  }

  case class InvalidPosition(msg: String) extends Positions {
    val boundingBox = EmptyExtent
  }

  case class Position(x: Double, y: Double) extends Positions {
    lazy val boundingBox = NonEmptyExtent(x,y,x,y)
  }

  case class PositionList(list: List[Positions]) extends Positions {
    lazy val boundingBox = list.foldLeft[Extent]( EmptyExtent ) { (ex, pos) => pos.expand(ex) }
  }

  implicit val extentFormats = new Format[Extent] {

    def toJsResult(array : JsArray) : JsResult[Extent] =
      if (array.value.isEmpty) JsSuccess(EmptyExtent)
      else
        Try{
          val xmin = array.value(0).as[Double]
          val ymin = array.value(1).as[Double]
          val xmax = array.value(2).as[Double]
          val ymax = array.value(3).as[Double]
          JsSuccess(NonEmptyExtent(xmin, ymin, xmax, ymax))
        }.getOrElse( JsError(ValidationError(s"Array $array can't be turned into a valid boundingbox")))

    def reads(json: JsValue): JsResult[Extent] = json match {
      case a : JsArray => toJsResult(a)
      case _ => JsError(ValidationError("extent must be an array of 4 numbers"))
    }

    def writes(ex: Extent): JsValue = JsArray(ex.toList.map( d => JsNumber(d)))

  }

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
          val pl = psv.foldLeft(ListBuffer[Positions]())( (result, el) => result :+ toPos(el) ).toList
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

  implicit val geometryTypeWrites = new Writes[GeometryType] {
    def writes(t: GeometryType): JsValue = JsString(
      t match {
        case GeometryType.POINT => "Point"
        case GeometryType.MULTI_POINT => "MultiPoint"
        case GeometryType.LINE_STRING => "LineString"
        case GeometryType.MULTI_LINE_STRING => "MultiLineString"
        case GeometryType.POLYGON => "Polygon"
        case GeometryType.MULTI_POLYGON => "MultiPolygon"
        case GeometryType.GEOMETRY_COLLECTION => "GeometryCollection"
      }
    )
  }

  implicit val PointCollectionWrites = new Writes[PointCollection] {

    import scala.collection.JavaConversions._

    def write(ps: PointSequence): JsArray =
      if (ps.isEmpty) Json.arr()
      else if (ps.size() == 1) Json.arr( ps.getX(0), ps.getY(0))
      else {
        val buf = ps.foldLeft( ListBuffer[JsArray]())( (buf, p) => {buf.append(Json.arr( p.getX, p.getY )); buf})
        JsArray(buf)
      }

    def writes(col: PointCollection): JsArray = col match {
      case ps : PointSequence => write(ps)
      case cc : ComplexPointCollection => {
        val buf = cc.getPointSets.foldLeft( ListBuffer[JsArray]() ) ( (buf, c) => {buf.append(writes(c)); buf} )
        JsArray(buf)
      }
    }
  }

  implicit val GeometryWithoutCrsWrites = new Writes[Geometry] {
    def writes(geometry: Geometry): JsValue = Json.obj(
    "type" -> geometry.getGeometryType,
    "coordinates" -> geometry.getPoints
    )
  }

}

