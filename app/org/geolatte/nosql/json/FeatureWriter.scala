package org.geolatte.nosql.json

import org.geolatte.common.Feature
import scala.concurrent.Future

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/5/13
 */
trait FeatureWriter {


  def add(f: Feature) : Future[Boolean]

  def flush() : Unit

}



