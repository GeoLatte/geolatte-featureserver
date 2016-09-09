package utilities

import featureserver.FeatureWriter
import play.Logger
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.{JsSuccess, _}
import play.api.mvc.{BodyParser, Result}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {

  /**
   * Result for the GeoJson parsing
 *
   * @param msg the state message
   * @param warnings the list with Warning messages
   * @param dataRemaining the remaing data (unparseable final part of the previous chunk)
   */
  case class State(msg: String = "", warnings: List[String] =  List(), dataRemaining: String = "")

  def parseAsString( json: JsValue, state: State, features: List[JsObject]) = json.validate[JsObject] match {
    case JsSuccess(f, _) => (f :: features, state)
    case JsError(seq) => (features, State("With Errors", JsonHelper.JsValidationErrors2String(seq) :: state.warnings, ""))
  }

  def processChunk(writer: FeatureWriter, state: State, chunk: Array[Byte], sep : String = "\n")
                  (implicit ec: ExecutionContext) : Future[State] = {
    val chunkAsString = new String(chunk, "UTF8")
    val toProcess = state.dataRemaining + chunkAsString
    val jsonStrings = toProcess.split(sep)
    Logger.debug(s"split results in ${jsonStrings.size} elements" )
    val (fs, newState) = jsonStrings.foldLeft(
        (List[JsObject](), state.copy(dataRemaining=""))
    ) ( (res : (List[JsObject], State), fStr : String )  => {
      val (features, curState) = res
      if (!curState.dataRemaining.isEmpty) Logger.warn(s"Invalid JSON: could not parse ${curState.dataRemaining}")
      Try{
        val json = Json.parse(fStr)
        parseAsString(json, curState.copy(dataRemaining = ""), features)
      }.getOrElse( (features, curState.copy(dataRemaining = fStr)) )
    })
    writer.add(fs).map( int => newState)
  }

  def mkStreamingIteratee(writer: FeatureWriter, sep: String)(implicit ec: ExecutionContext) : Iteratee[Array[Byte], Either[Result, Future[State]]] =
    Iteratee.fold( Future{ State() } ) {
      (fState: Future[State], chunk: Array[Byte]) => fState.flatMap( state => processChunk(writer, state, chunk, sep))
    }.map( fstate => Right(fstate) )

  def bodyParser(writer: FeatureWriter, sep: String)(implicit ec: ExecutionContext) = BodyParser("GeoJSON feature BodyParser") { request =>
    mkStreamingIteratee(writer, sep)
  }

}
