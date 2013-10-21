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

  def add(features: Seq[JsObject]): Unit

  def updateIndex(): Unit

}

case class MongoWriter(db: String, collection: String) extends FeatureWriter {


  type CollectionInfo = (JSONCollection, Metadata, Reads[JsObject])

  val fCollectionInfo: Future[CollectionInfo] = MongoRepository.getCollection(db, collection) map {
    case (dbcoll, smd) =>
      (dbcoll, smd, FeatureTransformers.mkFeatureIndexingTranformer(smd.envelope, smd.level))
    case _ => throw new DatabaseNotFoundException(s"$db/$collection not found, or collection is not spatially enabled")
  }

  def add(features: Seq[JsObject]) = {
    for ((collection, smd, transfo) <- fCollectionInfo) yield {
      import ExecutionContext.Implicits.global
      val docs = features.map(f => f.transform(transfo)).collect {
        case JsSuccess(transformed, _) => transformed
      }
      Logger.info(Thread.currentThread + " Flushing data to mongodb (" + docs.size + " features)")
      collection.bulkInsert(Enumerator.enumerate(docs)).onComplete {
        case Success(num) => Logger.info(s"Successfully inserted $num features")
        case Failure(f) => Logger.warn(s"Insert failed with error: ${f.getMessage}")
      }
    }
  }

  def updateIndex(): Unit = {
    import ExecutionContext.Implicits.global
    for ((collection, _, _) <- fCollectionInfo) {
      val idxManager = collection.indexesManager
      idxManager.ensure(new Index(Seq((SpecialMongoProperties.MC, Ascending)))) onFailure {
        case ex => Logger.warn("Failure on creating mortoncode index on collection %s" format collection.name)
      }
    }
  }

}