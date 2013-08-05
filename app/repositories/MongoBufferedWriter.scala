package repositories

import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb.MongoDbFeatureCollection
import org.geolatte.nosql.json.FeatureWriter

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
class MongoBufferedWriter (db: String, col: String)(implicit bufSize : Int = 1024) extends FeatureWriter {


  val writer = MongoRepository.getCollection(db,col) match {
    case (Some(dbcoll), Some(mc)) => new MongoDbFeatureCollection(dbcoll, mc)
    case _ => throw new RuntimeException()
  }

  def add(f: Feature) : Boolean = writer.add(f)

  def flush() : Unit = writer.flush()

}
