package controllers

import scala.language.implicitConversions

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import nosql.json.GeometryReaders._
import nosql.mongodb.{MediaReader, Media, Metadata}
import org.geolatte.geom.Envelope
import org.apache.commons.codec.binary.Base64
import play.api.libs.functional.ContravariantFunctor
import play.api.libs.iteratee.Enumerator

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
  def toJsonStream : Enumerator[JsObject]
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
  import Formats.CollectionFormat
  def toJson = Json.toJson(md)
}

case class MediaMetadataResource(id: String, md5: Option[String], url: String) extends Jsonable {
  import Formats.MediaMetadataWrites
  def toJson = Json.toJson(this)
}

case class MediaResource(id: String, name: String, md5: Option[String], data: String)

case class MediaReaderResource(mediaReader: MediaReader) extends Jsonable {
  import Formats.MediaReaderWrites
  def toJson = Json.toJson(mediaReader)
}

case class FeaturesResource(features: List[JsObject]) extends Jsonable {
  def toJson: JsValue = Json.obj ("count" -> features.length , "features" -> Json.arr(features))
}

object Formats {

  def toByteArray(implicit r: Reads[String]) : Reads[Array[Byte]] = r.map(str => Base64.decodeBase64(str))

  implicit val MediaReads : Reads[MediaObjectIn] = (
      ( __ \ "content-type").read[String] and
      ( __ \ "name").read[String] and
      ( __ \ "data").read[Array[Byte]](toByteArray)
    )(MediaObjectIn)

  implicit val MediaMetadataWrites : Writes[MediaMetadataResource] = (
    ( __ \ "id").write[String] and
    ( __ \ "md5").writeNullable[String] and
    ( __ \ "url").write[String]
    )(unlift(MediaMetadataResource.unapply))


  def mkMetadata(extent: Envelope, level: Int) = Metadata.apply("", extent, level)


  val CollectionReads: Reads[Metadata] = (
         (__ \ "extent").read(EnvelopeFormats) and
         (__ \ "index-level").read[Int](min(0))
       ) ( mkMetadata _ )

  val CollectionWrites :  Writes[Metadata] = (
     ( __ \ "collection").write[String] and
     ( __ \ "extent").write[Envelope] and
     ( __ \ "index-level").write[Int] and
     ( __ \ "count").write[Long]
   )(unlift(Metadata.unapply))

  implicit val CollectionFormat : Format[Metadata]  = Format(CollectionReads, CollectionWrites)

  implicit val MediaReaderWrites : Writes[MediaReader] = (
      (__ \ "id").write[String] and
      (__ \"md5").writeNullable[String] and
      (__ \ "name" ).write[String] and
      (__ \ "length").write[Int] and
      (__ \ "content-type").writeNullable[String] and
      (__ \ "data").write[String].contramap[Array[Byte]]( ar => Base64.encodeBase64String(ar))
    )(unlift(MediaReader.unapply))

}

