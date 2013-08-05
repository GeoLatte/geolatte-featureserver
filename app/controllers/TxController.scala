package controllers

import play.api.mvc.{BodyParsers, Action, Controller}
import org.geolatte.nosql.json.ReactiveGeoJson
import repositories.{MongoRepository, MongoBufferedWriter}
import com.mongodb.casbah.WriteConcern
import com.mongodb.util.{JSONParseException, JSON => Parser}
import com.mongodb.casbah.Imports._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {



  def insert(db: String, col: String) = {
    val writer = try {
      val writer = new MongoBufferedWriter(db, col)
      Some(ReactiveGeoJson.bodyParser(writer))
    } catch {
      case ex: Throwable => None
    }
    val parser = parse.using {
      rh =>
        writer.getOrElse(parse.error(NotFound(s"$db/$col does not exist or is not a spatial collection")))
    }
    Action(parser) {
      request => Ok(request.body.msg)
    }
  }

  def remove(db: String, col: String) = Action(BodyParsers.parse.tolerantText) {
    request => {
      val (collectionOpt, _) = MongoRepository.getCollection(db, col)
      val reqBody = toDBObject(request.body) match {
        case Right(body) => extractKey(body, "query")
        case Left(err) => Left(err)
      }
      (collectionOpt, reqBody) match {
        case (Some(collection), Right(doc)) =>
          val result = collection.remove(doc, WriteConcern.Safe)
          if (result.getLastError.ok) Ok(s"Deleted ${result.getN}")
          else InternalServerError(s"Delete error: ${result.getLastError.getErrorMessage}")

        case (None, _) => NotFound(s"$db/$col does not exist.")
        case (_, Left(err)) => BadRequest(err)
      }
    }
  }

  def toDBObject(txt: String): Either[String, MongoDBObject] = {
    try {
      Parser.parse(txt) match {
        case r: DBObject => Right(r)
        case _ => Left(s"'$txt' does not represent an object")
      }
    } catch {
      case e: JSONParseException => Left(s"'$txt' is not valid JSON")
    }
  }

  def extractKey(mobj: MongoDBObject, key: String): Either[String, MongoDBObject] = {
    mobj.get(key) match {
      case Some(value: DBObject) => Right(value)
      case Some(_) => Left(s"Query property is not an object")
      case None => Left(s"No query property in object")
    }
  }


}
