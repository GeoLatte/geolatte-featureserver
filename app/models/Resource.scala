package models


import repositories.{SpatialMetadata, Metadata}
import play.api.libs.json.{JsValue, Writes, Json}
import org.geolatte.nosql.mongodb.{EnvelopeSerializer, MetadataIdentifiers}
import controllers.routes

trait RenderableResource


trait Jsonable extends RenderableResource {
  def toJson: JsValue
}

trait Csvable extends RenderableResource {
  def toCsv: String
}

case class DatabasesResource(dbNames: Traversable[String]) extends Jsonable {
  lazy val intermediate = dbNames map (name => Map("name" -> name, "url" -> routes.Databases.getDb(name).url))

  def toJson = Json.toJson(intermediate)
}

case class DatabaseResource(db: String, collections: Traversable[String]) extends Jsonable {
  lazy val intermediate = collections map (name => Map("name" -> name, "url" -> routes.Databases.getCollection(db, name).url))

  def toJson = Json.toJson(intermediate)
}

case class CollectionResource(md: Metadata) extends Jsonable {

  implicit object MetadataWrites extends Writes[Metadata] {

    def writes(meta: Metadata): JsValue = meta.spatialMetadata match {
      case Some(smd) => Json.obj(
        "collection" -> meta.name,
        "num-objects" -> meta.count,
        "extent" -> EnvelopeSerializer(smd.envelope),
        "index-level" -> smd.level,
        "index-stats" -> Json.toJson(smd.stats))
      case None => Json.obj("collection" -> meta.name, "num-objects" -> meta.count)
    }
  }

  def toJson: JsValue = Json.toJson(md)

}
