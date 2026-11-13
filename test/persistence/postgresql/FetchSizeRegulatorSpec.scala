package persistence.postgresql

import org.specs2.mutable.Specification

import java.lang.reflect.{ InvocationHandler, Method, Proxy }
import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

class FetchSizeRegulatorSpec extends Specification {

  private val MB = 1024L * 1024

  /** A ResultSet that only records the fetch sizes it is given. */
  private def recordingResultSet(): (ResultSet, ListBuffer[Int]) = {
    val fetchSizes = ListBuffer.empty[Int]
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        if (method.getName == "setFetchSize") fetchSizes += args(0).asInstanceOf[Int]
        null
      }
    }
    val rs = Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[_]](classOf[ResultSet]), handler).asInstanceOf[ResultSet]
    (rs, fetchSizes)
  }

  "FetchSizeRegulator.fetchSizeFor" should {

    "use the maximum fetch size for small rows" in {
      FetchSizeRegulator.fetchSizeFor(8 * MB, 1024, 256) === 256
    }

    "divide the byte budget by the largest row" in {
      FetchSizeRegulator.fetchSizeFor(8 * MB, 500 * 1024, 256) === 16
    }

    "never go below one row, even when a row exceeds the budget" in {
      FetchSizeRegulator.fetchSizeFor(8 * MB, 25 * MB, 256) === 1
    }

    "cope with empty rows" in {
      FetchSizeRegulator.fetchSizeFor(8 * MB, 0, 256) === 256
    }
  }

  "A FetchSizeRegulator" should {

    "raise the fetch size to the maximum once it has seen that rows are small" in {
      val (rs, fetchSizes) = recordingResultSet()
      val regulator = new FetchSizeRegulator(8 * MB)
      regulator.observe(rs, 1024)
      fetchSizes.toList === List(256)
    }

    "lower the fetch size each time a larger row shows up" in {
      val (rs, fetchSizes) = recordingResultSet()
      val regulator = new FetchSizeRegulator(8 * MB)
      Seq(1024L, 500 * 1024L, 2 * MB).foreach(regulator.observe(rs, _))
      fetchSizes.toList === List(256, 16, 4)
    }

    "leave the fetch size alone for rows that are not larger than the largest seen" in {
      val (rs, fetchSizes) = recordingResultSet()
      val regulator = new FetchSizeRegulator(8 * MB)
      Seq(2 * MB, 1024L, 2 * MB, 100 * 1024L).foreach(regulator.observe(rs, _))
      fetchSizes.toList === List(4)
    }
  }

}
