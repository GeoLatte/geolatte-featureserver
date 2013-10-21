package nosql.mongodb

import org.geolatte.geom.Envelope

import play.api.libs.json._
import play.api.libs.functional.syntax._
import nosql.json.EnvelopeSerializer

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/24/13
 */

case class Metadata(name: String, count: Long, spatialMetadata: Option[SpatialMetadata] = None)

case class SpatialMetadata(envelope: Envelope, level : Int)

object SpatialMetadata {

  import MetadataIdentifiers._

  implicit val SpatialMetadataReads = (
    (__ \ ExtentField).read(EnvelopeSerializer.EnvelopeFormat) and
    (__ \ IndexLevelField).read[Int]
    )(SpatialMetadata.apply _)

}