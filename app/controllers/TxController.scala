package controllers

import play.api.mvc._
import scala.Some
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import reactivemongo.core.commands.{LastError, GetLastError}
import config.AppExecutionContexts
import org.geolatte.nosql.json.{MongoWriter, ReactiveGeoJson}
import scala.concurrent.Future
import repositories.MongoRepository
import play.api.libs.json.{Json, JsObject}
import org.codehaus.jackson.JsonParseException
import org.geolatte.nosql.json.ReactiveGeoJson.State
import play.modules.reactivemongo.json.BSONFormats.BSONDocumentFormat

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {

  /**
   * An update operation on a MongoCollection using the sequence of MongoDBObjects as arguments
   */
  type UpdateOp = (BSONCollection, Seq[BSONDocument]) => Future[LastError]

  import AppExecutionContexts.streamContext

  def insert(db: String, col: String) = {
    val parser = mkJsonWritingBodyParser(db, col)
    Action(parser) {
        request => Ok(request.body.warnings.mkString("\n"))
      }
  }

  def remove(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = extractKey(_, "query"),
    updateOp = (coll, arguments) => coll.remove(arguments(0), GetLastError(awaitJournalCommit = true))
  )

  def update(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = extractKey(_, "query", "update"),
    updateOp = (coll, arguments) => coll.update(arguments(0), arguments(1), GetLastError(awaitJournalCommit = true))
  )

  //  def reindex(db: String, col: String, level: Int) = {
  //    Action {
  //      request => MongoRepository.reindex(db, col, level)
  //    }
  //  }


  private def mkUpdateAction(db: String, col: String)
                            (extractor: BSONDocument => Seq[BSONDocument],
                             updateOp: UpdateOp) =
    Action(BodyParsers.parse.tolerantText) {
      implicit request => Async {
        val opArgumentsEither = toDBObject(request.body)(extractor)
        val pair = for (pairOpt <- MongoRepository.getCollection(db, col); (collectionOpt, _) = pairOpt) yield (collectionOpt, opArgumentsEither)
        pair.flatMap {
          p => p match {
            case (Some(collection), Right(opArguments)) => {
              updateOp(collection, opArguments).map(le => Ok(s"Result: ${le.ok}")).recover {
                case t: Throwable => InternalServerError(t.getMessage)
              }
            }
            case (None, _) => Future.successful(NotFound(s"$db/$col does not exist."))
            case (_, Left(err)) => Future.successful(BadRequest(err))
          }
        }
      }
    }



  private def toDBObject(txt: String)(implicit extractor: BSONDocument => Seq[BSONDocument]): Either[String, Seq[BSONDocument]] = {
    try {
      val jsValue = Json.parse(txt)
      jsValue match {
        case r: JsObject => {
          val doc = BSONDocumentFormat.partialReads(r).getOrElse(BSONDocument())
          Right(extractor(doc))
        }
        case _ => Left(s"'$txt' does not represent an object")
      }
    } catch {
      case e: JsonParseException => Left(s"'$txt' is not valid JSON")
      case e: RuntimeException => Left(e.getMessage)
    }
  }

  private def extractKey(mobj: BSONDocument, keys: String*): Seq[BSONDocument] =
    for (key <- keys) yield mobj.get(key) match {
      case Some(value: BSONDocument) => value
      case Some(_) => throw new RuntimeException(s"$key property is not an object")
      case None => throw new RuntimeException(s"No $key property in object")
    }

  /**
   * Creates a new BodyParser that deserializes GeoJson features in the input and
   * writes them to the specified database and collection.
   *
   * @param db the database to write features to
   * @param col the collection to write features to
   * @return a new BodyParser
   */
  private def mkJsonWritingBodyParser(db: String, col: String) : BodyParser[State] = {
      val writer = new MongoWriter(db, col)
      ReactiveGeoJson.bodyParser(writer)
  }


}

