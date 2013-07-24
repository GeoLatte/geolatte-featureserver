package models


import repositories.{NonSpatialMetadata, SpatialMetadata, Metadata}
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

    def writes(meta: Metadata): JsValue = meta match {
      case md: SpatialMetadata => Json.obj(
        "collection" -> md.name,
        "extent" -> EnvelopeSerializer(md.envelope),
        "index-level" -> md.level,
        "index-stats" -> Json.toJson(md.stats))
      case md: NonSpatialMetadata => Json.obj("collection" -> md.name)
    }
  }

  def toJson: JsValue = Json.toJson(md)

}
