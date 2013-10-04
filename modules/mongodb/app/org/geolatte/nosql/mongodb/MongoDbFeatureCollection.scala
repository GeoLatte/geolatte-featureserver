package org.geolatte.nosql.mongodb

import scala.Some
import org.geolatte.geom.curve.{MortonCode, MortonContext}
import org.geolatte.common.Feature
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import play.api.Logger
import play.api.libs.iteratee.Enumerator
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
case class MongoDbFeatureCollection(collection: BSONCollection, spatialMetadata: SpatialMetadata,
                                    bufSize: Int = 10000) {

  lazy val mortonContext = new MortonContext(spatialMetadata.envelope, spatialMetadata.level)
  lazy val mortoncode = new MortonCode(mortonContext)

  def add(features: Seq[Feature]): Unit = {
    val docs = features.map( f => convertFeature(f)).collect { case Some(doc) => doc }
    insert(docs)
  }

  def convertFeature(f: Feature): Option[BSONDocument] = {
    try {
      val mc = mortoncode ofGeometry f.getGeometry
      Some(MongoDbFeature(f, mc))
    } catch {
      case ex: IllegalArgumentException => {
        Logger.warn(s"Failed to convert feature $f to BSON: ${ex.getMessage}")
        None
      }
    }
  }

  def insert(docs : Seq[BSONDocument]) = {
    //TODO -- configure proper execution contexts
    import ExecutionContext.Implicits.global
    Logger.info(Thread.currentThread + " Flushing data to mongodb (" + docs.size + " features)")
    collection.bulkInsert(Enumerator.enumerate(docs)).onComplete {
      case Success(num) => Logger.info(s"Successfully inserted $num features")
      case Failure(f) => Logger.warn(s"Insert failed with error: ${f.getMessage}")
    }

  }

  def updateIndex() = {
    //TODO -- configure proper execution contexts
    import ExecutionContext.Implicits.global
    val idxManager = collection.indexesManager
    idxManager.ensure( new Index(Seq((SpecialMongoProperties.MC, Ascending)))) onFailure {
      case ex => Logger.warn("Failure on creating mortoncode index on collection %s" format collection.name)
    }
  }

}
