package controllers

import Exceptions.UnsupportedMediaException
import akka.stream.scaladsl.Source
import akka.util.ByteString
import config.Constants
import featureserver.json.GeometryReaders._
import featureserver.{ Metadata, MetadataIdentifiers }
import org.apache.commons.codec.binary.Base64
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.{ Envelope, Geometry, Point }
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext
import play.api.data.validation.ValidationError
import play.api.http.Writeable
import play.api.libs.functional.syntax._
import play.api.libs.iteratee.{ Enumerator, Input }
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.api.libs.streams.Streams
import play.api.mvc._
import utilities.EnumeratorUtility.CommaSeparate

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions

trait Resource

trait AsSource[A] {
  def asSource(implicit writeable: Writeable[A]): Source[ByteString, _]
}

object ResourceWriteables {

  def mkJsonWriteable[R <: Resource](toJson: R => JsValue)(implicit req: RequestHeader): Writeable[R] = {
    val ct = RequestContext(req)
    val (ctype, fmt) = ct.mediaType
    fmt match {
      case Constants.Format.JSON =>
        Writeable(r => ByteString.fromString(Json.stringify(toJson(r))), Some(ctype.toString))
      case _ => throw UnsupportedMediaException("No supported media type: " + ct.request.acceptedTypes.mkString(";"))
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

  /**
   * Creates a stateful Writeable for writing CSV
   * @param req the request header
   * @return
   */
  def mkCsVWriteable(implicit req: RequestHeader): Writeable[JsObject] = {
    implicit val ct = RequestContext(req)
    val encoder = new DefaultCsvEncoder()

    val cc = new CsvContext(0, 0, 0)

    def encode(v: JsString) = "\"" + encoder.encode(v.value, cc, CsvPreference.STANDARD_PREFERENCE).replaceAll("\n", "")
      .replaceAll("\r", "") + "\""

    def expand(v: JsValue): Seq[(String, String)] =
      utilities.JsonHelper.flatten(v.asInstanceOf[JsObject]) sortBy {
        case (k, _) => k
      } map {
        case (k, v: JsString) => (k, encode(v))
        case (k, v: JsNumber) => (k, Json.stringify(v))
        case (k, v: JsBoolean) => (k, Json.stringify(v))
        case (k, _) => (k, "")
      }

    def project(js: JsValue)(selector: PartialFunction[(String, String), String], geomToString: Geometry => String): Seq[String] = {
      val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
      val attributes = expand(jsObj).collect(selector)
      val geom = geomToString((js \ "geometry").asOpt(GeometryReads(CrsId.UNDEFINED)).getOrElse(Point.createEmpty()))
      val idOpt = (js \ "id").asOpt[String].map(v => ("id", v)).getOrElse(("id", "null"))
      selector(idOpt) +: geom +: attributes
    }

    implicit val queryStr = req.queryString
    val sep = ct.sep

    val toCsvRecord = (js: JsValue) => project(js)({
      case (k, v) => v
      case _ => "None"
    }, g => s""""${g.asText}"""").mkString(sep)

    val toCsvHeader = (js: JsValue) => project(js)({
      case (k, v) => k
      case _ => "None"
    }, _ => "geometry-wkt").mkString(sep)

    var i: Int = 0

    val toCsv: JsValue => ByteString = (js: JsValue) => {
      i = i + 1
      if (i == 1) ByteString.fromString(toCsvHeader(js) + "\n" + toCsvRecord(js))
      else ByteString.fromString(toCsvRecord(js))
    }

    Writeable((js: JsValue) => toCsv(js))
  }

  def selectWriteable(ct: RequestContext): (Writeable[JsObject], String) = {
    val (mediaType, fmt) = ct.mediaType
    fmt match {
      case Constants.Format.CSV => (mkCsVWriteable(ct.request), mediaType.toString)
      case Constants.Format.JSON =>
        val jsw = implicitly[Writeable[JsValue]]
        (jsw, mediaType.toString)
    }

  }

}

case class DatabasesResource(dbNames: Traversable[String]) extends Resource {
  lazy val json: JsValue = Json.toJson(
    dbNames map (name => Map("name" -> name, "url" -> routes.DatabasesController.getDb(name).url))
  )

}

case class DatabaseResource(db: String, collections: Traversable[String]) extends Resource {
  lazy val intermediate = collections map (name => Map("name" -> name, "url" -> routes.DatabasesController.getCollection(db, name).url))
  lazy val json = Json.toJson(intermediate)
}

case class CollectionResource(md: Metadata) extends Resource {
  lazy val json = Json.toJson(md)(Formats.CollectionWrites)
}

case class FeaturesResource(totalOpt: Option[Long], features: Enumerator[JsObject]) extends Resource with AsSource[JsObject] {

  override def asSource(implicit writeable: Writeable[JsObject]): Source[ByteString, _] = {
    val total: Long = totalOpt.getOrElse(-1L)
    val commaSeparate = new CommaSeparate(",")
    val enumerator = Enumerator(ByteString(s"""{ "total": $total, "features": [""")) >>>
      (features.map(writeable.transform) &> commaSeparate) >>>
      Enumerator(ByteString("]}"))
    Source.fromPublisher(Streams.enumeratorToPublisher(enumerator))
  }
}

case class FeatureStream(totalOpt: Option[Long], features: Enumerator[JsObject]) extends Resource with AsSource[JsObject] {

  override def asSource(implicit writeable: Writeable[JsObject]): Source[ByteString, _] = {
    val finalSeparatorEnumerator = Enumerator.enumerate(List(ByteString(config.Constants.chunkSeparator)))
    val commaSeparate = new CommaSeparate(config.Constants.chunkSeparator)
    val jsons = features.map(writeable.transform) &> commaSeparate
    val enumerator = jsons andThen finalSeparatorEnumerator andThen Enumerator.enumInput(Input.EOF)
    Source.fromPublisher(Streams.enumeratorToPublisher(enumerator))
  }

}

case class IndexDef(name: String, path: String, cast: String, regex: Boolean)

case class IndexDefsResource(dbName: String, colName: String, indexNames: Traversable[String]) extends Resource {
  lazy val intermediate = indexNames map (name => Map("name" -> name, "url" -> routes.IndexController.get(dbName, colName, name).url))
  def toJson = Json.toJson(intermediate)
}

case class IndexDefResource(dbName: String, colName: String, indexDef: IndexDef) extends Resource {
  import Formats.IndexDefWrites
  def toJson = Json.toJson(indexDef)
}

object Formats {

  def toByteArray(implicit r: Reads[String]): Reads[Array[Byte]] = r.map(str => Base64.decodeBase64(str))

  def newCollectionMetadata(extent: Envelope, level: Int, idtype: String) =
    Metadata.fromReads("", extent, level, idtype)

  def registerTableMetadata(collection: String, extent: Envelope, geometryCol: String) =
    Metadata(collection, extent, 0, "decimal", 0, geometryCol, "", jsonTable = false)

  /**
   * This is the format  for the PUT resource when creating a Json table
   */
  val CollectionReadsForJsonTable: Reads[Metadata] = (
    (__ \ MetadataIdentifiers.ExtentField).read(EnvelopeFormats) and
    (__ \ MetadataIdentifiers.IndexLevelField).read[Int](min(0)) and
    (__ \ MetadataIdentifiers.IdTypeField).read[String](
      Reads.filter[String](ValidationError("Requires 'text' or 'decimal"))(tpe => tpe == "text" || tpe == "decimal")
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

  def ViewDefOut(db: String, col: String) = (
    (__ \ 'name).json.pickBranch(of[JsString]) and
    (__ \ 'query).json.pickBranch(of[JsString]) and
    (__ \ 'projection).json.pickBranch(of[JsArray]) and
    (__ \ 'url).json.copyFrom((__ \ 'name).json.pick.map(name => JsString(controllers.routes.ViewController.get(db, col, name.as[String]).url)))
  ).reduce

  val ViewDefExtract = (
    (__ \ "query").readNullable[String] and
    (__ \ "projection").readNullable[JsArray]
  ).tupled

  implicit val IndexDefReads: Reads[IndexDef] = (
    (__ \ 'name).readNullable[String].map { _.getOrElse("") } and
    (__ \ 'path).read[String] and
    (__ \ 'type).read[String](filter[String](ValidationError("Type must be either 'text', 'bool' or 'decimal'"))(s => List("text", "bool", "decimal").contains(s))) and
    (__ \ 'regex).readNullable[Boolean].map(_.getOrElse(false))
  )(IndexDef)

  implicit val IndexDefWrites: Writes[IndexDef] = (
    (__ \ 'name).write[String] and
    (__ \ 'path).write[String] and
    (__ \ 'type).write[String] and
    (__ \ 'regex).write[Boolean]
  )(unlift(IndexDef.unapply))

  implicit val IndexDefFormat: Format[IndexDef] = Format(IndexDefReads, IndexDefWrites)

}

