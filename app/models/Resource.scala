package models



import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._

import controllers.routes
import _root_.util.SpatialSpec
import repositories.Metadata
import scala.Some
import play.api.data.validation.ValidationError

//TODO -- remove the EnvelopeSerializer dep.
import org.geolatte.nosql.mongodb.EnvelopeSerializer

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

object CollectionResourceReads {

def size(s: Int) = Reads.filter[Vector[Double]](ValidationError("validate.error: size not 4"))( _.size == s)

implicit val SpatialSpecReads: Reads[SpatialSpec] = (
    (__ \ "crs").read[Int] and
    (__ \ "extent").read[Vector[Double]](size(4)) and
    (__ \ "index-level").read[Int](min(0))
  ) (SpatialSpec)

}

