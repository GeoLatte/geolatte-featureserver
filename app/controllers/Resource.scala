package controllers

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.data.validation.ValidationError
import nosql.json.GeometryReaders._
import nosql.mongodb.Metadata
import Metadata._

trait RenderableResource


trait Jsonable extends RenderableResource {
  def toJson: JsValue
}

trait Csvable extends RenderableResource {
  def toCsv: String
}

case class SpatialSpec(crs: Int, extent: Vector[Double], level: Int) {
  def envelope: String = s"$crs:${extent(0)},${extent(1)},${extent(2)},${extent(3)}"
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

object CollectionResourceReads {

def size(s: Int) = Reads.filter[Vector[Double]](ValidationError("validate.error: size not 4"))( _.size == s)

implicit val SpatialSpecReads: Reads[SpatialSpec] = (
    (__ \ "crs").read[Int] and
    (__ \ "extent").read[Vector[Double]](size(4)) and
    (__ \ "index-level").read[Int](min(0))
  ) (SpatialSpec)

}

