package org.geolatte.nosql.json

import org.geolatte.common.Feature
import repositories.MongoRepository
import play.api.Logger
import org.geolatte.nosql.mongodb.{SpatialMetadata, SpecialMongoProperties, MongoDbFeature}
import scala.concurrent.{ExecutionContext, Future}

import config.AppExecutionContexts.streamContext
import org.geolatte.geom.curve.{MortonCode, MortonContext}
import reactivemongo.bson._
import play.api.libs.iteratee.Enumerator
import scala.util.{Failure, Success}
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import reactivemongo.api.collections.default.BSONCollection

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */
trait FeatureWriter {

  def add(features: Seq[Feature]): Unit

  def updateIndex(): Unit

}

case class MongoWriter(db: String, collection: String) extends FeatureWriter {


  type CollectionInfo = (BSONCollection, SpatialMetadata, MortonCode)

  val fCollectionInfo: Future[CollectionInfo] = MongoRepository.getCollection(db, collection) map {
    case (dbcoll, Some(smd)) =>
      lazy val mortonContext = new MortonContext(smd.envelope, smd.level)
      lazy val mortoncode = new MortonCode(mortonContext)
      (dbcoll, smd, mortoncode)
    case _ => throw new RuntimeException(s"$db/$collection not found, or collection is not spatially enabled")
  }


  def convertFeature(f: Feature, mc: MortonCode): Option[BSONDocument] = {
    try {
      val h = mc ofGeometry f.getGeometry
      Some(MongoDbFeature(f, h))
    } catch {
      case ex: IllegalArgumentException => {
        Logger.warn(s"Failed to convert feature $f to BSON: ${ex.getMessage}")
        None
      }
    }
  }

  def add(features: Seq[Feature]) = {
    for ((collection, smd, mc) <- fCollectionInfo) yield {
      import ExecutionContext.Implicits.global
      val docs = features.map(f => convertFeature(f, mc)).collect {
        case Some(f) => f
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