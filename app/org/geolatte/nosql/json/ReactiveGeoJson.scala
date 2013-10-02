package org.geolatte.nosql.json

import play.api.mvc.BodyParser
import org.geolatte.nosql.json.GeometryReaders._
import org.geolatte.geom.crs.CrsId
import play.api.libs.json.{JsPath, JsError, JsSuccess, Json}
import play.api.data.validation.ValidationError
import org.geolatte.common.Feature
import play.api.libs.iteratee.Iteratee
import play.Logger
import config.ConfigurationValues
import scala.concurrent.ExecutionContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {

  /**
   * Result for the GeoJson parsing
   * @param msg the state message
   * @param warnings the list with Warning messages
   */
  case class State(msg: String = "", warnings: List[String] =  List(), dataRemaining: String = "")

  /**
   * Converts a Json validation error sequence for a Feature into a single error message String.
   * @param errors
   * @return
   */
  def proccessErrors(errors: Seq[(JsPath, Seq[ValidationError])]): String = {
    errors map {
      case (jspath, valerrors) => jspath + " :" + valerrors.map(ve => ve.message).mkString("; ")
    } mkString("\n")
  }

  /**
   * Parses a String representation of a GeoJson Feature to a Feature
   *
   * In case
   * @param featureStr
   * @return
   */
  def parseFeatureString(crs: CrsId,  featureStr: String) : Either[(String, String), Feature] = {
//    Logger.debug(s"feature string is ($crs): " + featureStr)
    implicit val featureReader = FeatureReads(crs)
    try {
      val jsVal = Json.parse(featureStr)
      jsVal.validate[Feature] match {
        case JsSuccess(f, _)  => Right(f)
        case JsError(errors) => Left(proccessErrors(errors), "")
      }
    } catch {
      case ex : Throwable => {
        Logger.debug("Exception in parsing: " + ex)
        Left( (ex.getMessage, featureStr) )
      }
    }
  }

  def processChunk(crs: CrsId, writer: FeatureWriter, state: State, chunk: Array[Byte]) : State = {
    val chunkAsString = new String(chunk, "UTF8")
    Logger.debug("Processing Chunk....")
//    Logger.debug("Incoming chunk is: " + chunkAsString)
//    Logger.debug("Previous remaining state is: " + state.dataRemaining)
    val toProcess = state.dataRemaining + chunkAsString

    val featureStrings = toProcess.split(ConfigurationValues.jsonSeparator)
    Logger.debug(s"split results in ${featureStrings.size} elements" )

    val parseResults = for {
      fs <- featureStrings
    } yield parseFeatureString(crs, fs)


    val validBsons = parseResults.collect {
      case Right(f) => f
    }

    writer.add(validBsons)


    //if the last warning is a json parse exception (identified by the Left having a non-empty second element
    // then the error-message (if any) should not be included in the warnings list (since it will be reparsed
    //on the next chunk
    val newWarnings = parseResults.last match {
      case Left((_, "")) => parseResults collect { case Left((msg, _)) => msg }
      case _ => parseResults.slice(0, parseResults.size -1) collect { case Left((msg, _)) => msg }
    }

    //potentially the last string in featureStrings fails to parse because it is not complete
    //in that case the whole string should be reparsed on the next iteration.
    val remainingData = parseResults.last match {
      case Left((_, rem)) => rem
      case _ => ""
    }

    State(state.msg, state.warnings ++ newWarnings, remainingData)
  }

  def mkStreamingIteratee(crs: CrsId, writer: FeatureWriter) =
    Iteratee.fold( State() ) { (state: State, chunk: Array[Byte]) => processChunk(crs, writer, state, chunk) } mapDone( state => Right(state) )

  def bodyParser(crs: CrsId, writer: FeatureWriter)(implicit ec: ExecutionContext) = BodyParser("GeoJSON feature BodyParser") { request =>
    mkStreamingIteratee(crs, writer)
  }

}
