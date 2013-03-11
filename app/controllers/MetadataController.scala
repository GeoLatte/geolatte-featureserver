package controllers

import play.api.mvc.{Result, Action, Controller}
import repositories.MongoRepository
import play.api.libs.json.{JsValue, Writes, Json}
import org.geolatte.nosql.mongodb.{EnvelopeSerializer, MetadataIdentifiers, Metadata}


/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/11/13
 */
object MetadataResource extends Controller {

  implicit object MetadataWrites extends Writes[Metadata] {

      import MetadataIdentifiers._

      def writes(md: Metadata): JsValue = Json.obj(

          CollectionField -> md.name,
          ExtentField -> EnvelopeSerializer(md.envelope),
          IndexLevelField -> md.level,
          IndexStatsField -> Json.toJson(md.stats)
      )
    }

    def metadata(db: String, collection: String) = Action {
      request => mkResponse(db,collection)
    }

   def mkResponse(db: String, coll: String) : Result =
   try {
    val md = MongoRepository.metadata(db,coll)
    Ok(Json.toJson(md))
   }  catch {
     case ex : NoSuchElementException => NotFound
   }


}
