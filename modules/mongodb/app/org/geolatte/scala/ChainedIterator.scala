package org.geolatte.scala

/**
 * An iterator that iterates over each iterator in a lazy Stream of iterators
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/6/13
 */
class ChainedIterator[A](val iterators : Stream[Iterator[A]]) extends Iterator[A] {


  private val it = iterators.iterator
  private var currentSubIt : Iterator[A] =  if (it.hasNext) it.next else Iterator.empty

  def hasNext: Boolean =
     (currentSubIt.hasNext) ||
       {
         if (!it.hasNext) false
         else {
          currentSubIt = it.next
          hasNext
         }
       }

  def next(): A =
    if (! hasNext) throw new NoSuchElementException()
    else currentSubIt.next

}

object ChainedIterator {

  def chain[A](iterators: Iterator[A] *) : ChainedIterator[A] = new ChainedIterator(iterators.toStream)

  def chain[A](iterators: Stream[Iterator[A]]) : ChainedIterator[A] = new ChainedIterator(iterators)

}
