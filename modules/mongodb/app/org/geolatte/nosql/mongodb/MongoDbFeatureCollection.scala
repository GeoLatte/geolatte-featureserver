package org.geolatte.nosql.mongodb

import scala.Some
import org.geolatte.geom.curve.{MortonCode, MortonContext}
import org.geolatte.common.Feature
import scala.collection.mutable.ArrayBuffer
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONDocument
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import play.api.Logger
import scala.concurrent.ExecutionContext
import play.api.libs.iteratee.Enumerator

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
case class MongoDbFeatureCollection(collection: BSONCollection, spatialMetadata: SpatialMetadata,
                                    bufSize: Int = 10000) {

  lazy val mortonContext = new MortonContext(spatialMetadata.envelope, spatialMetadata.level)
  lazy val mortoncode = new MortonCode(mortonContext)

  private val buffer = new ArrayBuffer[BSONDocument](initialSize = bufSize)

  def add(f: Feature): Boolean = convertFeature(f) match {
    case Some(obj) => buffer += obj
      if (buffer.size == bufSize) flush()
      true
    case None => {
      Logger.info("Warning: failure to read feature with envelope" + f.getGeometry.getEnvelope)
      false
    }
  }

  def convertFeature(f: Feature): Option[BSONDocument] = {
    try {
      val mc = mortoncode ofGeometry f.getGeometry
      Some(MongoDbFeature(f, mc))
    } catch {
      case ex: IllegalArgumentException => None
    }
  }

  def flush(): Unit = {
    //TODO -- configure proper execution contexts
    import ExecutionContext.Implicits.global
    collection.bulkInsert(Enumerator.enumerate(buffer))
    Logger.info(Thread.currentThread + " Flushing data to mongodb")
    buffer.clear

    //TODO this is duplicate code from MongoDbSink

    val idxManager = collection.indexesManager
    val futureComplete = idxManager.create( new Index(Seq((SpecialMongoProperties.MC, Ascending))))

    futureComplete.onFailure {
      case ex => Logger.warn("Failure on creating mortoncode index on collection %s" format collection.name)
    }
    //END duplicate

  }

}
