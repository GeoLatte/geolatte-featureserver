package org.geolatte.nosql.json

import play.api.mvc.BodyParser
import org.geolatte.nosql.json.GeometryReaders._
import org.geolatte.geom.crs.CrsId
import play.api.libs.json._
import play.api.data.validation.ValidationError
import org.geolatte.common.Feature
import play.api.libs.iteratee.Iteratee
import play.Logger
import config.ConfigurationValues
import scala.concurrent.ExecutionContext
import play.api.libs.json.JsSuccess
import play.api.data.validation.ValidationError
import reactivemongo.bson.BSONDocument
import org.codehaus.jackson.JsonParseException

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {

  /**
   * Result for the GeoJson parsing
   * @param msg the state message
   * @param warnings the list with Warning messages
   * @param dataRemaining the remaing data (unparseable final part of the previous chunk)
   * @param featureReads the current Reads[Feature] to use
   */
  case class State(msg: String = "",
                   warnings: List[String] =  List(),
                   dataRemaining: String = "",
                   featureReads : Reads[Feature] = FeatureReads(CrsId.valueOf(4326)))

  /**
   * Converts a Json validation error sequence for a Feature into a single error message String.
   * @param errors
   * @return
   */
  def processErrors(errors: Seq[(JsPath, Seq[ValidationError])]): String = {
    errors map {
      case (jspath, valerrors) => jspath + " :" + valerrors.map(ve => ve.message).mkString("; ")
    } mkString("\n")
  }

  def parseAsString( json: JsValue, state: State, features: List[Feature])(implicit fr : Reads[Feature]) = json.validate[Feature] match {
    case JsSuccess(f, _) => (f :: features, state)
    case JsError(seq) => (features, State("With Errors", processErrors(seq) :: state.warnings, "", state.featureReads  ))
  }

  def processChunk(writer: FeatureWriter, state: State, chunk: Array[Byte]) : State = {
    val chunkAsString = new String(chunk, "UTF8")
//    Logger.debug("Processing Chunk....")
//    Logger.debug(chunkAsString)
    val toProcess = state.dataRemaining + chunkAsString

    val featureStrings = toProcess.split(ConfigurationValues.jsonSeparator)
    Logger.debug(s"split results in ${featureStrings.size} elements" )

    val crsDeclarationReads = (__ \ "crs").read[CrsId]

    val (fs, newState) = featureStrings.foldLeft( (List[Feature](), state.copy(dataRemaining="")) )( (res : (List[Feature], State), fStr : String )  => {
      val (features, curState) = res
      if (!curState.dataRemaining.isEmpty) Logger.warn(s"Invalid JSON: could not parse ${curState.dataRemaining}")
      try {
        val json = Json.parse(fStr)
        val optCrsDeclaration = json.asOpt[CrsId](crsDeclarationReads)
        optCrsDeclaration match {
          case Some(newCrs) => {
            Logger.info("Setting CRS to " + newCrs)
            ( features , State( curState.msg, curState.warnings, "", FeatureReads(newCrs) ) )
          }
          case None => parseAsString(json, curState.copy(dataRemaining = ""), features)(curState.featureReads)
        }
      } catch {
        case ex : JsonParseException => (features, curState.copy(dataRemaining = fStr))
      }
    })

    writer.add(fs)
    newState

  }

  def mkStreamingIteratee(writer: FeatureWriter) =
    Iteratee.fold( State() ) { (state: State, chunk: Array[Byte]) => processChunk(writer, state, chunk) } mapDone( state => Right(state) )

  def bodyParser(writer: FeatureWriter)(implicit ec: ExecutionContext) = BodyParser("GeoJSON feature BodyParser") { request =>
    mkStreamingIteratee(writer)
  }

}
