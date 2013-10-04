package org.geolatte.nosql.json

import org.geolatte.common.Feature
import repositories.MongoRepository
import play.api.Logger
import org.geolatte.nosql.mongodb.MongoDbFeatureCollection
import scala.concurrent.Future

import config.AppExecutionContexts.streamContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */
class MongoWriter(db: String, collection: String) extends FeatureWriter {

  val futureWriter = MongoRepository.getCollection(db, collection) map {
      case (Some(dbcoll), Some(mc)) =>
        Logger.info("Creating writer for features.")
        new MongoDbFeatureCollection(dbcoll, mc)
      case _ => throw new RuntimeException(s"$db/$collection not found, or collection is not spatially enabled")
    }

  def add(features: Seq[Feature]) = {
    for (writer <- futureWriter) yield writer.add(features)
  }

  def updateIndex() : Unit = {
    for (write <- futureWriter) {
      write.updateIndex()
    }
  }

}