package controllers

import util.MediaTypeSpec
import play.api.mvc.{Action, Controller}
import repositories.MongoRepository
import play.api.libs.json._
import config.ConfigurationValues.{Format, Version}

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 7/22/13
 */
object Databases extends Controller {



 def get() = {
   Action { implicit request =>
    val dbs = MongoRepository.listDatabases()
    request match {
       case MediaTypeSpec(Format.JSON, version)  => Ok(Json.toJson(dbs)).as( MediaTypeSpec(Format.JSON, version) )
       case _  => UnsupportedMediaType("No supported media type.")
     }
   }
 }



}
