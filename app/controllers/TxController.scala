//package controllers
//
//import play.api.mvc._
//import org.geolatte.nosql.json.ReactiveGeoJson
//import repositories.{MongoRepository, MongoBufferedWriter}
//import com.mongodb.casbah.WriteConcern
//import com.mongodb.util.{JSONParseException, JSON => Parser}
//import com.mongodb.casbah.Imports._
//import com.mongodb.WriteResult
//import scala.Some
//import scala.Some
//
///**
// * @author Karel Maesen, Geovise BVBA
// *         creation-date: 7/25/13
// */
//object TxController extends Controller {
//
//  /**
//   * An update operation on a MongoCollection using the sequence of MongoDBObjects as arguments
//   */
//  type UpdateOp = (MongoCollection, Seq[MongoDBObject]) => WriteResult
//
//  import play.api.libs.concurrent.Execution.Implicits._
//
//  def insert(db: String, col: String) = {
//    Action(mkJsonWritingBodyParser(db, col)) {
//      request => Ok(request.body.msg)
//    }
//  }
//
//  def remove(db: String, col: String) = mkUpdateAction(db, col)(
//    extractor = extractKey(_, "query"),
//    updateOp = (coll, arguments) => coll.remove(arguments(0), WriteConcern.Safe))
//
//  def update(db: String, col: String) = mkUpdateAction(db,col)(
//    extractor = extractKey(_,"query", "update"),
//    updateOp = (coll, arguments) => coll.update(arguments(0), arguments(1), concern=WriteConcern.Safe) )
//
//  def reindex(db: String, col: String, level: Int) = {
//    Action {
//      request => MongoRepository.reindex(db, col, level)
//    }
//  }
//
//
//  private def mkUpdateAction( db: String, col: String)
//                            (extractor: MongoDBObject => Seq[MongoDBObject],
//                             updateOp : UpdateOp ) =
//    Action(BodyParsers.parse.tolerantText) {
//    implicit request => {
//      val (collectionOpt, _) = MongoRepository.getCollection(db, col)
//      val opArgumentsEither = toDBObject(request.body) (extractor)
//      (collectionOpt, opArgumentsEither) match {
//        case (Some(collection), Right(opArguments)) =>
//          val result = updateOp(collection, opArguments)
//          if (result.getLastError.ok) Ok(s"Records affected ${result.getN}")
//          else InternalServerError(s"Database error: ${result.getLastError.getErrorMessage}")
//
//        case (None, _) => NotFound(s"$db/$col does not exist.")
//        case (_, Left(err)) => BadRequest(err)
//      }
//    }
//  }
//
//  private def toDBObject(txt: String)(implicit extractor: MongoDBObject => Seq[MongoDBObject]): Either[String, Seq[MongoDBObject]] = {
//    try {
//      Parser.parse(txt) match {
//        case r: DBObject => Right(extractor(r))
//        case _ => Left(s"'$txt' does not represent an object")
//      }
//    } catch {
//      case e: JSONParseException => Left(s"'$txt' is not valid JSON")
//      case e: RuntimeException => Left(e.getMessage)
//    }
//  }
//
//  private def extractKey(mobj: MongoDBObject, keys: String*): Seq[MongoDBObject] =
//    for( key <- keys) yield mobj.get(key) match {
//      case Some(value: DBObject) => new MongoDBObject(value)
//      case Some(_) => throw new RuntimeException(s"$key property is not an object")
//      case None => throw new RuntimeException(s"No $key property in object")
//    }
//
//  /**
//   * Creates a new BodyParser that deserializes GeoJson features in the input and
//   * writes them to the specified database and collection.
//   *
//   * @param db the database to write features to
//   * @param col the collection to write features to
//   * @return a new BodyParser
//   */
//  private def mkJsonWritingBodyParser(db: String, col: String) = try {
//    val writer = new MongoBufferedWriter(db, col)
//    ReactiveGeoJson.bodyParser(writer)
//  } catch {
//    case ex: Throwable => parse.error(NotFound(s"$db/$col does not exist or is not a spatial collection"))
//  }
//
//}
