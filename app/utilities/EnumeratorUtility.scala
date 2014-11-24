package utilities

import play.api.libs.iteratee.{Enumeratee, Enumerator}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by Karel Maesen, Geovise BVBA on 24/11/14.
 */
object EnumeratorUtility {


  /**
   * Transforms an Enumerator into one that enumerates pairs of a (zero-based) index and the
   * original element.
   *
   * @param inner the transformed Enumerator
   * @param dex the Execution context for the enumerator
   * @tparam E type of the Elements of the inner Enumerator
   * @return Enumerator of (index,element)-pairs
   */
  def withIndex[E](inner : Enumerator[E])(implicit dex : ExecutionContext) : Enumerator[(Int, E)] = {
    var counter = -1  //captured by the closure over the composed Enumerator that is returned
    inner.through[(Int, E)](Enumeratee.map {
      e => {counter = counter +1; (counter,e)}
    })
  }

}
