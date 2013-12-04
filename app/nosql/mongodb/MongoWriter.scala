package nosql.mongodb

import play.api.Logger
import scala.concurrent._
import play.modules.reactivemongo.json.collection.JSONCollection
import play.api.libs.iteratee.Enumerator
import scala.util.{Failure, Success}
import play.api.libs.json._
import nosql.mongodb.Repository.CollectionInfo

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */
trait FeatureWriter {

  def add(features: Seq[JsObject]): Future[Int]

}

case class MongoWriter(db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  val fCollectionInfo: Future[CollectionInfo] = Repository.getCollectionInfo(db, collection)

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

}