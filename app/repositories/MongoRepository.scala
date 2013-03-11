package repositories

import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve.{MortonContext, MortonCode}
import org.geolatte.geom.Envelope
import com.mongodb.casbah.MongoClient
import org.geolatte.common.Feature
import org.geolatte.nosql.mongodb.MongoDbSource

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/11/13
 */

  //this needs to move to a service layer
  object MongoRepository {

    //these are temporary -- need to be injected
    var wgs84 = CrsId.valueOf(4326)
    val mortoncode = new MortonCode(new MortonContext(new Envelope(-140.0, 15, -40.0, 50.0, wgs84), 8))
    val mongo = MongoClient()

    def query(database: String, collection: String, window: Envelope): Iterator[Feature] = {
      val coll = mongo(database)(collection)
      val src = MongoDbSource(coll, mortoncode)
      src.query(window)
    }

  }
