package controllers

import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import nosql.json.GeometryReaders._
import nosql.mongodb.Metadata
import org.geolatte.geom.Envelope

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
  import CollectionResource.CollectionFormat
  def toJson: JsValue = Json.toJson(md)(CollectionFormat)
}

object CollectionResource {

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

  implicit val CollectionFormat = Format(CollectionReads, CollectionWrites)


}


