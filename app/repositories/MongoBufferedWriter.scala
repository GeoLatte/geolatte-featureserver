package repositories

import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb.MongoDbFeatureCollection
import org.geolatte.nosql.json.FeatureWriter
import play.api.Logger


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
class MongoBufferedWriter (db: String, col: String) extends FeatureWriter {


  val writer = MongoRepository.getCollection(db,col) match {
    case (Some(dbcoll), Some(mc)) =>
      Logger.info("Creating writer for features.")
      new MongoDbFeatureCollection(dbcoll, mc)
    case _ => throw new RuntimeException()
  }

  def add(f: Feature) : Boolean = writer.add(f)

  def flush() : Unit = {
    writer.flush()
  }

}
