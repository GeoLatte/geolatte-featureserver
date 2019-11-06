package controllers

import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import Exceptions.UnsupportedMediaException
import akka.stream.scaladsl.Source
import akka.util.ByteString
import config.Constants
import org.geolatte.geom.{ Envelope, Geometry, Point }
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext
import persistence.GeoJsonFormats._
import persistence._
import play.api.http.{ MediaType, Writeable }
import play.api.libs.functional.syntax._
import play.api.libs.json.Json._
import play.api.libs.json.JsonValidationError
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.api.mvc._

trait Resource

trait AsSource[A] {
  def asSource(implicit writeable: Writeable[A]): Source[ByteString, _]
}

object ResourceWriteables {

  def selectWriteable(
    req:  RequestHeader,
    qFmt: Option[Constants.Format.Formats],
    sep:  Option[String]                   = None
  ): (Writeable[JsObject], String) = {
    val (fmt, mediaType) = getFormat(req, qFmt)
    fmt match {
      case Constants.Format.CSV => (mkCsVWriteable(req, sep), mediaType.toString)
      case Constants.Format.JSON =>
        val jsw = implicitly[Writeable[JsValue]]
        (jsw, mediaType.toString)
    }

  }

  def getFormat(req: RequestHeader, qFmt: Option[Constants.Format.Value]): (Constants.Format.Value, MediaType) = {

    val (fmt, version) = (qFmt, req) match {
      case (Some(f), _)                    => (f, Constants.Version.default)
      case (_, SupportedMediaTypes(f, ve)) => (f, ve)
      case _                               => (Constants.Format.JSON, Constants.Version.default)
    }

    val ctype = SupportedMediaTypes(fmt, version)
    (fmt, ctype)
  }

  /**
   * Creates a stateful Writeable for writing CSV
   *
   * @param req the request header
   * @return
   */
  def mkCsVWriteable(implicit req: RequestHeader, sepOpt: Option[String] = None): Writeable[JsObject] = {
    val encoder = new DefaultCsvEncoder()
    val sep = sepOpt.getOrElse(config.Constants.separator)
    val cc = new CsvContext(0, 0, 0)

    def encode(v: JsString) = "\"" + encoder.encode(v.value, cc, CsvPreference.STANDARD_PREFERENCE).replaceAll(
      "\n",
      " "
    )
      .replaceAll("\r", " ") + "\""

    def expand(v: JsValue): Seq[(String, String)] =
      utilities.JsonHelper.flatten(v.asInstanceOf[JsObject]) sortBy {
        case (k, _) => k
      } map {
        case (k, v: JsString)  => (k, encode(v))
        case (k, v: JsNumber)  => (k, Json.stringify(v))
        case (k, v: JsBoolean) => (k, Json.stringify(v))
        case (k, _)            => (k, "")
      }

    def project(js: JsValue)(selector: PartialFunction[(String, String), String], geomToString: Geometry => String): Seq[String] = {
      val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
      val attributes = expand(jsObj).collect(selector)
      val geom = geomToString((js \ "geometry").asOpt(GeoJsonFormats.geometryReads).getOrElse(Point
        .createEmpty()))
      val id = "id" -> (js \ "id").getOrElse(JsString("<No Id>")).toString
      selector(id) +: geom +: attributes
    }

    val toCsvRecord = (js: JsValue) => project(js)({
      case (k, v) => v
      case _      => "None"
    }, g => s""""${g.asText}"""").mkString(sep)

    val toCsvHeader = (js: JsValue) => project(js)({
      case (k, v) => k
      case _      => "None"
    }, _ => "geometry-wkt").mkString(sep)

    var i: Int = 0

    val toCsv: JsValue => ByteString = (js: JsValue) => {
      i = i + 1
      if (i == 1) {
        ByteString.fromString(toCsvHeader(js) + "\n" + toCsvRecord(js))
      } else {
        ByteString.fromString(toCsvRecord(js))
      }
    }

    Writeable((js: JsValue) => toCsv(js))
  }

  def mkJsonWriteable[R <: Resource](toJson: R => JsValue)(implicit req: RequestHeader): Writeable[R] = {
    val (fmt, ctype) = getFormat(req, None)
    fmt match {
      case Constants.Format.JSON =>
        Writeable(r => ByteString.fromString(Json.stringify(toJson(r))), Some(ctype.toString))
      case _ => throw UnsupportedMediaException("No supported media type: " + req.acceptedTypes.mkString(";"))
    }
  }

  implicit def DatabasesResourceWriteable(implicit req: RequestHeader): Writeable[DatabasesResource] =
    mkJsonWriteable(r => r.json)

  implicit def DatabaseResourceWriteable(implicit req: RequestHeader): Writeable[DatabaseResource] =
    mkJsonWriteable(db => db.json)

  implicit def CollectionResourceWriteable(implicit req: RequestHeader): Writeable[CollectionResource] =
    mkJsonWriteable(c => c.json)

  implicit def IndexDefResourceWriteable(implicit req: RequestHeader): Writeable[IndexDefResource] =
    mkJsonWriteable(d => d.toJson)

  implicit def IndexDefsResourceWriteable(implicit req: RequestHeader): Writeable[IndexDefsResource] =
    mkJsonWriteable(d => d.toJson)

}

case class DatabasesResource(dbNames: Traversable[String]) extends Resource {
  lazy val json: JsValue = Json.toJson(
    dbNames map (name => Map("name" -> name, "url" -> routes.DatabasesController.getDb(name).url))
  )

}

case class DatabaseResource(db: String, collections: Traversable[String]) extends Resource {
  lazy val intermediate = collections map (name => Map(
    "name" -> name,
    "url" -> routes.DatabasesController.getCollection(db, name)
      .url
  ))
  lazy val json = Json.toJson(intermediate)
}

case class CollectionResource(md: Metadata) extends Resource {
  lazy val json = Json.toJson(md)(Formats.CollectionWrites)
}

case class FeaturesResource(totalOpt: Option[Long], features: Source[JsObject, _])
  extends Resource with AsSource[JsObject] {
  private val end = ByteString.fromString(s"]}")
  private val sep = ByteString.fromString(",")

  override def asSource(implicit writeable: Writeable[JsObject]): Source[ByteString, _] = {
    val total: Long = totalOpt.getOrElse(-1L)
    val start = ByteString.fromString(s"""{"total": $total, "features": [""")
    features.map(writeable.transform).intersperse(start, sep, end)
  }
}

case class FeatureStream(totalOpt: Option[Long], features: Source[JsObject, _])
  extends Resource with AsSource[JsObject] {

  override def asSource(implicit writeable: Writeable[JsObject]): Source[ByteString, _] = {
    val chunkSep = ByteString.fromString(config.Constants.chunkSeparator)
    val finalSeparator = Source.single(chunkSep)
    val jsons = features.map(writeable.transform).intersperse(chunkSep)
    jsons.concat(finalSeparator)
  }

}

case class IndexDef(name: String, path: String, cast: String, regex: Boolean)

case class IndexDefsResource(dbName: String, colName: String, indexNames: Traversable[String]) extends Resource {
  lazy val intermediate = indexNames map (name => Map("name" -> name, "url" -> routes.IndexController.get(
    dbName,
    colName,
    name
  ).url))

  def toJson = Json.toJson(intermediate)
}

case class IndexDefResource(dbName: String, colName: String, indexDef: IndexDef) extends Resource {

  import Formats.IndexDefWrites

  def toJson = Json.toJson(indexDef)
}

object Formats {

  /**
   * This is the format  for the PUT resource when creating a Json table
   */
  val CollectionReadsForJsonTable: Reads[Metadata] = (
    (__ \ MetadataIdentifiers.ExtentField).read(EnvelopeFormats) and
    (__ \ MetadataIdentifiers.IndexLevelField).read[Int](min(0)) and
    (__ \ MetadataIdentifiers.IdTypeField).read[String](
      Reads
        .filter[String](JsonValidationError("Requires 'text' or 'decimal"))(tpe => tpe == "text" || tpe == "decimal")
    )
  )(newCollectionMetadata _)
  val CollectionReadsForRegisteredTable: Reads[Metadata] = (
    (__ \ MetadataIdentifiers.CollectionField).read[String] and
    (__ \ MetadataIdentifiers.ExtentField).read(EnvelopeFormats) and
    (__ \ MetadataIdentifiers.GeometryColumnField).read[String]
  )(registerTableMetadata _)
  val CollectionWrites: Writes[Metadata] = (
    (__ \ MetadataIdentifiers.CollectionField).write[String] and
    (__ \ MetadataIdentifiers.ExtentField).write[Envelope] and
    (__ \ MetadataIdentifiers.IndexLevelField).write[Int] and
    (__ \ MetadataIdentifiers.IdTypeField).write[String] and
    (__ \ MetadataIdentifiers.CountField).write[Long] and
    (__ \ MetadataIdentifiers.GeometryColumnField).write[String] and
    (__ \ MetadataIdentifiers.PkeyField).write[String] and
    (__ \ MetadataIdentifiers.IsJsonField).write[Boolean]
  )(unlift(Metadata.unapply))
  val projection: Reads[JsArray] = (__ \ "projection").readNullable[JsArray].map(js => js.getOrElse(Json.arr()))
  val ViewDefIn = (
    (__ \ 'query).json.pickBranch(of[JsString]) and
    (__ \ 'projection).json.copyFrom(projection)
  ).reduce
  val ViewDefExtract = (
    (__ \ "query").readNullable[String] and
    (__ \ "projection").readNullable[JsArray]
  ).tupled

  def newCollectionMetadata(extent: Envelope, level: Int, idtype: String) =
    Metadata.fromReads("", extent, level, idtype)

  def registerTableMetadata(collection: String, extent: Envelope, geometryCol: String) =
    Metadata(collection, extent, 0, "decimal", 0, geometryCol, "", jsonTable = false)

  def ViewDefOut(db: String, col: String) = (
    (__ \ 'name).json.pickBranch(of[JsString]) and
    (__ \ 'query).json.pickBranch(of[JsString]) and
    (__ \ 'projection).json.pickBranch(of[JsArray]) and
    (__ \ 'url).json.copyFrom((__ \ 'name).json.pick.map(name => JsString(controllers.routes.ViewController
      .get(db, col, name.as[String]).url)))
  ).reduce

  implicit val IndexDefReads: Reads[IndexDef] = (
    (__ \ 'name).readNullable[String].map {
      _.getOrElse("")
    } and
    (__ \ 'path).read[String] and
    (__ \ 'type).read[String](filter[String](JsonValidationError("Type must be either 'text', 'bool' or 'decimal'"))(
      s => List("text", "bool", "decimal").contains(s)
    )) and
    (__ \ 'regex).readNullable[Boolean].map(_.getOrElse(false))
  )(IndexDef)

  implicit val IndexDefWrites: Writes[IndexDef] = (
    (__ \ 'name).write[String] and
    (__ \ 'path).write[String] and
    (__ \ 'type).write[String] and
    (__ \ 'regex).write[Boolean]
  )(unlift(IndexDef.unapply))

  implicit val IndexDefFormat: Format[IndexDef] = Format(IndexDefReads, IndexDefWrites)

  def timestampToString(t: Timestamp): String = t.toLocalDateTime.format(DateTimeFormatter.ISO_DATE_TIME)

  def stringToTimestamp(dt: String): Timestamp = {
    val oft = OffsetDateTime.parse(dt, DateTimeFormatter.ISO_DATE_TIME)
    new Timestamp(oft.toEpochSecond)
  }

  implicit val timestampFormat = new Format[Timestamp] {

    def writes(t: Timestamp): JsValue = toJson(timestampToString(t))

    def reads(json: JsValue): JsResult[Timestamp] = fromJson[String](json).map(stringToTimestamp)

  }

  implicit val TableStatsFormat: Format[TableStats] = Json.format[TableStats]

  implicit val ActivityStatsFormat: Format[ActivityStats] = Json.format[ActivityStats]

}

