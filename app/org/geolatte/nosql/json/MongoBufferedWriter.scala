package org.geolatte.nosql.json

import org.geolatte.common.Feature
import repositories.MongoRepository
import play.api.Logger
import org.geolatte.nosql.mongodb.MongoDbFeatureCollection
import scala.concurrent.Future

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 9/13/13
 */
class MongoBufferedWriter(db: String, collection: String) extends FeatureWriter {

  val futureWriter = MongoRepository.getCollection(db, collection) map {
      case (Some(dbcoll), Some(mc)) =>
        Logger.info("Creating writer for features.")
        new MongoDbFeatureCollection(dbcoll, mc)
      case _ => throw new RuntimeException()
    }

  def add(f: Feature): Future[Boolean] = {
    for (writer <- futureWriter) yield writer.add(f)
  }

  def flush() {
    for(writer <- futureWriter) yield writer.flush()
  }

}
