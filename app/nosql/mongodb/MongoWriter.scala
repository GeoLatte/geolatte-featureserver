package nosql.mongodb

import play.api.Logger
import scala.concurrent._
import play.modules.reactivemongo.json.collection.JSONCollection
import config.AppExecutionContexts.streamContext
import play.api.libs.iteratee.Enumerator
import scala.util.{Failure, Success}
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import play.api.libs.json._
import nosql.Exceptions.DatabaseNotFoundException

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */
trait FeatureWriter {

  def add(features: Seq[JsObject]): Future[Int]

  def updateIndex(): Future[Boolean]

}

case class MongoWriter(db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  type CollectionInfo = (JSONCollection, Metadata, Reads[JsObject])

  val fCollectionInfo: Future[CollectionInfo] = MongoRepository.getCollection(db, collection) map {
    case (dbcoll, smd) =>
      (dbcoll, smd, FeatureTransformers.mkFeatureIndexingTranformer(smd.envelope, smd.level))
    case _ => throw new DatabaseNotFoundException(s"$db/$collection not found, or collection is not spatially enabled")
  }

  def add(features: Seq[JsObject]) =
    fCollectionInfo.flatMap{ case (coll, smd, transfo) => {
      val docs = features.map(f => f.transform(transfo)).collect {
        case JsSuccess(transformed, _) => transformed
      }
      Logger.debug(" Flushing data to mongodb (" + docs.size + " features)")
      val fInt = coll.bulkInsert(Enumerator.enumerate(docs))
      fInt.onComplete {
        case Success(num) => Logger.info(s"Successfully inserted $num features")
        case Failure(f) => Logger.warn(s"Insert failed with error: ${f.getMessage}")
      }
      fInt
    }
  }

  def updateIndex(): Future[Boolean] =
    fCollectionInfo.flatMap {
      case (coll, _ , _) => {
        val idxManager = coll.indexesManager
        val fResult = idxManager.ensure(new Index(Seq((SpecialMongoProperties.MC, Ascending))))
        fResult onFailure {
          case ex => Logger.warn("Failure on creating mortoncode index on collection %s" format coll.name)
        }
        fResult
      }
    }

}