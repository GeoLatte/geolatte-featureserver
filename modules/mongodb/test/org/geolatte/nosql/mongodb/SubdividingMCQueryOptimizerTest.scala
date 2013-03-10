package org.geolatte.nosql.mongodb

import org.specs2.mutable._
import org.geolatte.geom.curve._
import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import com.mongodb.casbah.Imports._

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/7/13
 */
class SubdividingMCQueryOptimizerTest extends Specification {

  val wgs84 = CrsId.valueOf(4326)

  val ctxt = new MortonContext(new Envelope(0, 0, 100, 100, wgs84), 3)
  val mortoncode = new MortonCode(ctxt)
  val optimizer = new SubdividingMCQueryOptimizer(){}

  "The optimizer given a window with morton code of maximal length" should {

    val window = new Envelope(2,2,3,3, wgs84)

    "return the mortoncode of the window" in {
        val result = optimizer.optimize(window, mortoncode)
        result must contain(DBObject("_mc" -> "000"))
        result must contain(DBObject("_mc" -> "00"))
        result must contain(DBObject("_mc" -> "0"))
        result must contain(DBObject("_mc" -> ""))
        result.size must_== 4
    }
  }

  "The optimizer given a small query window at the center" should {
    val window = new Envelope(49, 49, 51, 51, wgs84)

    "4 queries at lowest level " in {
      val result = optimizer.optimize(window, mortoncode)
      result must contain(DBObject("_mc" -> ""))
      result must contain(DBObject("_mc" -> "0"))
      result must contain(DBObject("_mc" -> "1"))
      result must contain(DBObject("_mc" -> "2"))
      result must contain(DBObject("_mc" -> "3"))
      result must contain(DBObject("_mc" -> "03"))
      result must contain(DBObject("_mc" -> "12"))
      result must contain(DBObject("_mc" -> "30"))
      result must contain(DBObject("_mc" -> "21"))
      result must contain(DBObject("_mc" -> "033"))
      result must contain(DBObject("_mc" -> "122"))
      result must contain(DBObject("_mc" -> "300"))
      result must contain(DBObject("_mc" -> "211"))
      result.size must_== 13

    }

  }


}
