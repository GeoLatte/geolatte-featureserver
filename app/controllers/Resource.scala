package controllers

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.data.validation.ValidationError
import nosql.json.GeometryReaders._
import nosql.mongodb.Metadata
import org.geolatte.geom.Envelope
import Metadata._


trait RenderableResource


trait Jsonable extends RenderableResource {
  def toJson: JsValue
}

trait Csvable extends RenderableResource {
  def toCsv: String
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


  implicit object MetadataWrites extends Writes[Metadata] {

    def writes(meta: Metadata): JsValue =  Json.obj(
        "collection" -> meta.name,
        "num-objects" -> meta.count,
        "extent" -> Json.toJson(meta.envelope),
        "index-level" -> meta.level
    )
  }

  def toJson: JsValue = Json.toJson(md)
}

object CollectionResource {

  def mkMetadata(extent: Envelope, level: Int) = Metadata.apply("", extent, level)

  val Reads: Reads[Metadata] = (
      (__ \ "extent").read(EnvelopeFormats) and
      (__ \ "index-level").read[Int](min(0))
    ) ( mkMetadata _ )

}


