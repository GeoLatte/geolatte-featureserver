package org.geolatte.scala

import org.specs2.mutable._
import java.util.NoSuchElementException

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/6/13
 */
class ChainedIteratorTest extends Specification {

  "The ChainedIterator built of an empty stream" should {
    def it = new ChainedIterator[String](Stream.empty)

    "return 0 elements " in {
      it.hasNext mustEqual false
    }

    "throw NoSuchElementException when next() is called" in {
      it.next() must throwA[NoSuchElementException]
    }

  }

  "The ChainedIterator built of a stream of possibly non-empty iterators" should {
      def it1 = List("a", "b", "c").iterator
      def it2 = List().iterator
      def it3 = List("d", "e").iterator
      //use def, rather than val so that the iterator is recreated for each test case
      def it = new ChainedIterator(List(it1, it2, it3).toStream)

      "return all elements " in {
        it.toList.size must_== 5
      }

      "return elements in order of iterators" in {
        it.take(3).toList must contain("a","b","c").only.inOrder
        val last = it.drop(3)
        last.toList mustEqual List("d", "e")
      }
    }

  "The ChainedIterator built of a stream of empty iterators" should {
        def it1 = List().iterator
        def it2 = List().iterator
        def it3 = List().iterator
        //use def, rather than val so that the iterator is recreated for each test case
        def it = new ChainedIterator(List(it1, it2, it3).toStream)

        "return 0 elements " in {
          it.hasNext mustEqual false
        }

      }


}
