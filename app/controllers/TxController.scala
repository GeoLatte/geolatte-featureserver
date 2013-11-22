package controllers

import play.api.mvc._
import reactivemongo.core.commands.{LastError, GetLastError}
import config.AppExecutionContexts
import scala.concurrent.Future
import play.api.libs.json._
import org.codehaus.jackson.JsonParseException
import play.modules.reactivemongo.json.collection.JSONCollection
import scala.util.Try
import nosql.mongodb._
import nosql.mongodb.ReactiveGeoJson._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends AbstractNoSqlController {

  /**
   * An update operation on a MongoCollection using the sequence of MongoDBObjects as arguments
   */
  type UpdateOp = (JSONCollection, Seq[JsValue]) => Future[LastError]

  import AppExecutionContexts.streamContext

  def insert(db: String, col: String) = {
    val parser = mkJsonWritingBodyParser(db, col)
    Action(parser) {
      request => Async {
        request.body.map( state =>  Ok(state.warnings.mkString("\n")))
      }
    }
  }

  def remove(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = extractKey(_, "query"),
    updateOp = (coll, arguments) => coll.remove(arguments(0), GetLastError(awaitJournalCommit = true)),
    "remove"
  )

  def update(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = extractKey(_, "query", "update"),
    updateOp = (coll, arguments) => coll.update(arguments(0), arguments(1), GetLastError(awaitJournalCommit = true),
      upsert= false, multi = true),
    "Update"
  )

  private def mkUpdateAction(db: String, col: String)
                            (extractor: JsValue => Seq[JsValue],
                             updateOp: UpdateOp,
                             updateOpName: String) =
    Action(BodyParsers.parse.tolerantText) {
      implicit request => Async {
        val opArgumentsEither = toDBObject(request.body)(extractor)
        repository.getCollection(db, col).flatMap( c =>
          (c, opArgumentsEither) match {
            case ( (collection, _), Right(opArguments)) => {
              updateOp(collection, opArguments)
            }
            case (_, Left(err)) => throw new IllegalArgumentException(s"Problem with update action arguments: $err")
          }
        ).map(le => {
          Ok(s"$updateOpName succeeded, with ${le.updated} documents affected.")
        }).recover(commonExceptionHandler(db,col))
      }
    }

  private def toDBObject(txt: String)(implicit extractor: JsValue => Seq[JsValue]): Either[String, Seq[JsValue]] = {
    Try {
      val jsValue = Json.parse(txt)
      jsValue match {
        case r: JsObject => Right(extractor(r))
        case _ => Left(s"'$txt' does not represent an object")
      }
    }.recover {
      case e: JsonParseException => Left(s"'$txt' is not valid JSON")
      case e: RuntimeException => Left(e.getMessage)
    }.get
  }

  private def extractKey(mobj: JsValue, keys: String*): Seq[JsValue] =
    for {
      key <- keys
      kVal =  mobj \ key
      if !kVal.isInstanceOf[JsUndefined]
    } yield kVal

  /**
   * Creates a new BodyParser that deserializes GeoJson features in the input and
   * writes them to the specified database and collection.
   *
   * @param db the database to write features to
   * @param col the collection to write features to
   * @return a new BodyParser
   */
  private def mkJsonWritingBodyParser(db: String, col: String): BodyParser[Future[State]] = {
    val writer = new MongoWriter(db, col)
    ReactiveGeoJson.bodyParser(writer)
  }


}

