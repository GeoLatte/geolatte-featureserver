package controllers

import featureserver.json.GeometryReaders._
import featureserver.{ Metadata, MetadataIdentifiers }
import org.apache.commons.codec.binary.Base64
import org.geolatte.geom.Envelope
import play.api.data.validation.ValidationError
import play.api.libs.functional.syntax._
import play.api.libs.iteratee.Enumerator
import play.api.libs.json.Reads._
import play.api.libs.json._
import utilities.EnumeratorUtility.CommaSeparate

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions

trait RenderableResource
trait RenderableNonStreamingResource extends RenderableResource
trait RenderableStreamingResource extends RenderableResource

trait Jsonable extends RenderableNonStreamingResource {
  def toJson: JsValue
}

trait Csvable extends RenderableNonStreamingResource {
  def toCsv: String
}

trait JsonStreamable extends RenderableStreamingResource {
  def toJsonStream: Enumerator[JsObject]
}

trait JsonStringStreamable extends RenderableStreamingResource {
  def toJsonStringStream: Enumerator[String]
}

trait CsvStreamable extends RenderableStreamingResource {
  def toCsvStream: Enumerator[String]
}

case class DatabasesResource(dbNames: Traversable[String]) extends Jsonable {
  lazy val intermediate = dbNames map (name => Map("name" -> name, "url" -> routes.DatabasesController.getDb(name).url))
  def toJson = Json.toJson(intermediate)
}

case class DatabaseResource(db: String, collections: Traversable[String]) extends Jsonable {
  lazy val intermediate = collections map (name => Map("name" -> name, "url" -> routes.DatabasesController.getCollection(db, name).url))
  def toJson = Json.toJson(intermediate)
}

case class CollectionResource(md: Metadata) extends Jsonable {
  def toJson = Json.toJson(md)(Formats.CollectionWrites)
}

case class FeaturesResource(totalOpt: Option[Long], features: Enumerator[JsObject]) extends JsonStringStreamable {
  def toJsonStringStream: Enumerator[String] = {
    val total: Long = totalOpt.getOrElse(-1L)
    val commaSeparate = new CommaSeparate(",")
    Enumerator(s"""{ "total": $total, "features": [""") >>>
      (features.map(Json.stringify) &> commaSeparate) >>>
      Enumerator("]}")
  }
}

case class IndexDef(name: String, path: String, cast: String, regex: Boolean)

case class IndexDefsResource(dbName: String, colName: String, indexNames: Traversable[String]) extends Jsonable {
  lazy val intermediate = indexNames map (name => Map("name" -> name, "url" -> routes.IndexController.get(dbName, colName, name).url))
  def toJson = Json.toJson(intermediate)
}

case class IndexDefResource(dbName: String, colName: String, indexDef: IndexDef) extends Jsonable {
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

