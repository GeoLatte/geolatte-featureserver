package nosql.mongodb

import play.api.Logger
import scala.concurrent._
import play.modules.reactivemongo.json.collection.JSONCollection
import play.api.libs.iteratee.Enumerator
import scala.util.{Failure, Success}
import play.api.libs.json._
import nosql.mongodb.Repository.CollectionInfo
import utilities.JsonHelper

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
    if(features.isEmpty) Future.successful(0)
    else {
      fCollectionInfo.flatMap{ case (coll, smd, transfo) => {
        //Logger.debug("Received as input: " + features)  //even for debug this produces way too much loggin data
        val docs = features.map(f => f.transform(transfo)).collect {
          case JsSuccess(transformed, _) => transformed
        }
        Logger.debug(" Flushing data to mongodb (" + docs.size + " features)")
        val fInt = coll.bulkInsert(Enumerator.enumerate(docs))
        fInt.onSuccess {
          case num => Logger.info(s"Successfully inserted $num features")
        }
        fInt.recover {
          case t : Throwable => { Logger.warn(s"Insert failed with error: ${t.getMessage}"); 0 }
        }
      }
    }
  }

}