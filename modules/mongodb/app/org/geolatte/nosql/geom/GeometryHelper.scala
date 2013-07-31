package org.geolatte.nosql.geom

import org.geolatte.geom._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/31/13
 */
object GeometryHelper {

  def toPointSequence(seq:  Point*) : PointSequence = seq match {
    case Seq() => EmptyPointSequence.INSTANCE
    case _ => {
      val fp = seq.head
      val b = PointSequenceBuilders.variableSized(fp.getDimensionalFlag, fp.getCrsId)
      seq.foldLeft(b)( (b,p) => b.add(p))
      b.toPointSequence
    }
  }

}
