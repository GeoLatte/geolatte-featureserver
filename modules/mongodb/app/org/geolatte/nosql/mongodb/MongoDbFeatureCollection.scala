package org.geolatte.nosql.mongodb

import com.mongodb.casbah.Imports._
import scala.Some
import org.geolatte.geom.curve.{MortonCode, MortonContext}
import org.geolatte.common.Feature
import play.Logger
import scala.collection.mutable.ArrayBuffer

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
case class MongoDbFeatureCollection(collection: MongoCollection, spatialMetadata: SpatialMetadata,
                                    bufSize: Int = 1024) {

  lazy val mortonContext = new MortonContext(spatialMetadata.envelope, spatialMetadata.level)
  lazy val mortoncode = new MortonCode(mortonContext)
  private lazy val mcStats = scala.collection.mutable.Map(spatialMetadata.stats.toSeq: _*)


  private val buffer = new ArrayBuffer[DBObject](initialSize = bufSize)

  def add(f: Feature): Boolean = convertFeature(f) match {
    case Some(obj) => buffer += obj
      if (buffer.size == bufSize) flush()
      true
    case None => false
  }

  def convertFeature(f: Feature): Option[DBObject] = {
    try {
      val mc = mortoncode ofGeometry f.getGeometry
      spatialMetadata.stats.put(mc, mcStats.getOrElse(mc, 0) + 1)
      Some(MongoDbFeature(f, mc))
    } catch {
      case ex: IllegalArgumentException => None
    }
  }

  def flush(): Unit = {
    collection.insert(buffer: _*)
    buffer.clear
    updateMetadata()
  }

  private def updateMetadata(): Unit = {

    collection.ensureIndex(SpecialMongoProperties.MC)
    import MetadataIdentifiers._

    val metadata = MongoDBObject(
      CollectionField -> collection.getName(),
      IndexStatsField -> mcStats,
      ExtentField -> EnvelopeSerializer(mortonContext.getExtent),
      IndexLevelField -> mortonContext.getDepth

    )
    val selector = MongoDBObject("collection" -> collection.getName())

    collection.getDB().getCollection(MetadataIdentifiers.MetadataCollection)
      .update(selector, metadata, true, false, WriteConcern.Safe)
  }


}
