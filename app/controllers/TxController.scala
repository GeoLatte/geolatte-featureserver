package controllers

import play.api.mvc.{BodyParsers, Action, Controller}
import org.geolatte.nosql.json.ReactiveGeoJson
import repositories.{MongoRepository, MongoBufferedWriter}
import com.mongodb.casbah.WriteConcern
import com.mongodb.util.{JSONParseException, JSON => Parser}
import com.mongodb.casbah.Imports._
import com.mongodb.WriteResult
import scala.Some

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {

  type UpdateOp = (MongoCollection, Seq[MongoDBObject]) => WriteResult
  import play.api.libs.concurrent.Execution.Implicits._

  def insert(db: String, col: String) = {
    val jsonWritingBodyParser = try {
      val writer = new MongoBufferedWriter(db, col)
      Some(ReactiveGeoJson.bodyParser(writer))
    } catch {
      case ex: Throwable => None
    }
    val parser = parse.using {
      rh =>
        jsonWritingBodyParser.getOrElse(parse.error(NotFound(s"$db/$col does not exist or is not a spatial collection")))
    }
    Action(parser) {
      request => Ok(request.body.msg)
    }
  }

  def remove(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = extractKey(_, "query"),
    updateOp = (coll, doc) => coll.remove(doc(0), WriteConcern.Safe))

  def update(db: String, col: String) = mkUpdateAction(db,col)(
    extractor = extractKey(_,"query", "update"),
    updateOp = (coll, doc) => coll.update(doc(0), doc(1), concern=WriteConcern.Safe) )


  def mkUpdateAction( db: String, col: String) (extractor: MongoDBObject => Seq[MongoDBObject],
                                                updateOp : UpdateOp ) =
    Action(BodyParsers.parse.tolerantText) {
    implicit request => {
      val (collectionOpt, _) = MongoRepository.getCollection(db, col)
      val reqBody = toDBObject(request.body) (extractor)
      (collectionOpt, reqBody) match {
        case (Some(collection), Right(doc)) =>
          val result = updateOp(collection, doc)
          if (result.getLastError.ok) Ok(s"Records affected ${result.getN}")
          else InternalServerError(s"Database error: ${result.getLastError.getErrorMessage}")

        case (None, _) => NotFound(s"$db/$col does not exist.")
        case (_, Left(err)) => BadRequest(err)
      }
    }
  }

  def toDBObject(txt: String)(implicit extractor: MongoDBObject => Seq[MongoDBObject]): Either[String, Seq[MongoDBObject]] = {
    try {
      Parser.parse(txt) match {
        case r: DBObject => Right(extractor(r))
        case _ => Left(s"'$txt' does not represent an object")
      }
    } catch {
      case e: JSONParseException => Left(s"'$txt' is not valid JSON")
      case e: RuntimeException => Left(e.getMessage)
    }
  }

  def extractKey(mobj: MongoDBObject, keys: String*): Seq[MongoDBObject] =
    for( key <- keys) yield mobj.get(key) match {
      case Some(value: DBObject) => new MongoDBObject(value)
      case Some(_) => throw new RuntimeException(s"$key property is not an object")
      case None => throw new RuntimeException(s"No $key property in object")
    }



}
