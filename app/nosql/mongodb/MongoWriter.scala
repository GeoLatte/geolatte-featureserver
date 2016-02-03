package nosql.mongodb

import nosql.FeatureWriter
import nosql.mongodb.MongoDBRepository.CollectionInfo
import org.reactivemongo.play.json._    //whatever IDEA says, we need this
import play.api.Logger
import play.api.libs.json._

import scala.concurrent._


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */

case class MongoWriter(db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  val fCollectionInfo: Future[CollectionInfo] = MongoDBRepository.getCollectionInfo(db, collection)

  def add(features: Seq[JsObject]) : Future[Long] =
    if(features.isEmpty) Future.successful(0)
    else
      fCollectionInfo.flatMap{ case (coll, smd, transfo) =>
        val docs = features
          .map( _.transform(transfo) )
          .collect {
            case JsSuccess(transformed, _) => transformed
          }

        Logger.debug(" Flushing data to mongodb (" + docs.size + " features)")

        import coll.ImplicitlyDocumentProducer._
        val bulkDocs = docs.map (implicitly[coll.ImplicitlyDocumentProducer](_)) //for this map see: https://groups.google.com/forum/#!topic/reactivemongo/dslaGJfxd6s

        val fInt = coll.bulkInsert(ordered = true)( bulkDocs:_* ).map(i => i.n.toLong)

        fInt.onSuccess {
          case num => Logger.info(s"Successfully inserted $num features")
        }

        fInt.recover {
          case t : Throwable =>
            Logger.warn(s"Insert failed with error: ${t.getMessage}")
            0L
        }
    }

}