package controllers

import play.api.mvc._
import reactivemongo.core.commands.{LastError, GetLastError}
import config.AppExecutionContexts
import scala.concurrent.Future
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.util.Try
import nosql.mongodb.ReactiveGeoJson._
import play.api.data.validation.ValidationError
import utilities.JsonHelper
import nosql.Exceptions.InvalidParamsException
import nosql.mongodb.{ReactiveGeoJson, MongoWriter, Repository}
import nosql.mongodb.Repository._
import reactivemongo.bson.BSONInteger
import com.fasterxml.jackson.core.JsonParseException

//TODO -- this should use repository, rather than directly perform updates

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends AbstractNoSqlController {

  val awaitJournalCommit = GetLastError(j = true, w = Some(BSONInteger(1)))

  /**
   * An update operation on a MongoCollection using the sequence of MongoDBObjects as arguments
   */
  type UpdateOp = (CollectionInfo, TxParams) => Future[LastError]

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
    extractor = js => TxParams((js \"query").as[JsObject]) ,
    updateOp = (collInfo, args) => {
      val (coll, _, _) = collInfo
      coll.remove(args.selectorDoc, awaitJournalCommit)
    },
    "remove"
  )

  def update(db: String, col: String) = mkUpdateAction(db, col)(
    extractor = js => TxParams( (js \ "query").as[JsObject], (js \"update").asOpt[JsObject]),
    updateOp = (collInfo, args) => {
      val (coll, _, featureTransformer) = collInfo
      if (!args.isUpdateDocWithOnlyOperators) throw InvalidParamsException("Only operators allowed in update document when updating.")
      args.getUpdateDoc(featureTransformer) match {
        case Right(updateDocJs) =>
          coll.update(args.selectorDoc, updateDocJs, awaitJournalCommit, upsert= false, multi = true)
        case Left(seq) => throw InvalidParamsException(JsonHelper.JsValidationErrors2String(seq))
      }
    },
    "Update"
  )

  def upsert(db: String, col: String) = mkUpdateAction(db, col)(
      extractor = js => TxParams( Json.obj("id" -> (js \ "id").as[JsValue]), js.asOpt[JsObject] ),
      updateOp = (collInfo, args) => {
          val (coll, _, featureTransformer) = collInfo
          if( !args.hasUpdateDoc) throw InvalidParamsException("Request body does not contain an object")
          if (!args.isUpdateDocWithOnlyFields) throw InvalidParamsException("Only fields allowed in update document when upserting.")
          args.getUpdateDoc(featureTransformer) match {
            case Right(updateDocJs) =>
              coll.update(args.selectorDoc, updateDocJs, awaitJournalCommit, upsert= true, multi = false)
            case Left(seq) => sys.error(s"Error on input: ${args.updateDoc} \n: ${JsonHelper.JsValidationErrors2String(seq)}")
          }
        },
      "Upsert"
    )

  private def mkUpdateAction(db: String, col: String)
                            (extractor: JsValue => TxParams,
                             updateOp: UpdateOp,
                             updateOpName: String) =
    Action(BodyParsers.parse.tolerantText) {
      implicit request => Async {
        val opArgumentsEither = toDBObject(request.body)(extractor)
        Repository.getCollectionInfo(db, col).flatMap( c =>
          (c, opArgumentsEither) match {
            case ( collInfo, Right(opArguments)) => {
              updateOp(collInfo, opArguments)
            }
            case (_, Left(err)) => throw new IllegalArgumentException(s"Problem with update action arguments: $err")
          }
        ).map(le => {
          Ok(s"$updateOpName succeeded, with ${le.updated} documents affected.")
        }).recover(commonExceptionHandler(db,col))
      }
    }

  private def toDBObject(txt: String)(implicit extractor: JsValue => TxParams): Either[String, TxParams] = {
    Try {
      val jsValue = Json.parse(txt)
      jsValue match {
        case r: JsObject => Right(extractor(r))
        case _ => Left(s"'$txt' does not represent an object")
      }
    }.recover {
      case e: JsonParseException => Left(s"'$txt' is not valid JSON")
      case t: Throwable => Left(t.getMessage)
    }.get
  }

  case class TxParams(selectorDoc: JsObject = Json.obj(), updateDoc: Option[JsObject] = None) {

    def hasUpdateDoc : Boolean = updateDoc.isDefined

    def isUpdateDocWithOnlyOperators : Boolean =
      updateDoc.exists(ud => ud.keys.filterNot(k => k.startsWith("$")).isEmpty)

    def isUpdateDocWithOnlyFields : Boolean =
          updateDoc.exists(ud => ud.keys.filter(k => k.startsWith("$")).isEmpty)

    def getUpdateDoc(trans: Reads[JsObject]) = {
      if (isUpdateDocWithOnlyOperators) Right(updateDoc.get)
      else if (isUpdateDocWithOnlyFields) {
        updateDoc.get.transform(trans).asEither
      } else {
        Left(Seq((__ \ "update",Seq(ValidationError("No update document, or mixture of fields and operators")))))
      }
    }

  }


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

