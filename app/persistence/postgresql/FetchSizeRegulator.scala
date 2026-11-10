package persistence.postgresql

import java.sql.ResultSet

/**
 * Keeps the memory held by a single JDBC fetch near a byte budget, to bound the heap a running
 * query can take: small rows are fetched many at a time, large rows few at a time. With the default budget of 8 MB, rows up to 32 KB are
 * fetched 256 at a time, rows of 500 KB 16 at a time, and rows of 8 MB or more one by one.
 *
 * The driver keeps every row of a fetch in memory until the next fetch, so a fixed fetch size
 * scales that buffer with the row size: 256 rows of 500 KB is 128 MB per running query. The
 * row size is unknown up front and differs per collection, so the first fetch is small and
 * every later fetch is sized from the largest row seen so far.
 *
 * One instance serves one result set. It only ever lowers the fetch size after the first
 * adjustment, and never fails a query: a row larger than the budget is simply fetched alone.
 */
class FetchSizeRegulator(targetBytes: Long, maxFetchSize: Int = FetchSizeRegulator.MaxFetchSize) {

  @volatile private var largestRowBytes = 0L

  /** Call for every row read; `rowBytes` is the approximate size of that row. */
  def observe(rs: ResultSet, rowBytes: Long): Unit =
    if (rowBytes > largestRowBytes) {
      largestRowBytes = rowBytes
      rs.setFetchSize(FetchSizeRegulator.fetchSizeFor(targetBytes, rowBytes, maxFetchSize))
    }

}

object FetchSizeRegulator {

  /** Fetch size of the first fetch, when nothing is known yet about the row size. */
  val InitialFetchSize = 8

  val MaxFetchSize = 256

  /**
   * A chosen value, not a tuned one. Large enough that rows up to 32 KB keep the maximum fetch
   * size, so ordinary collections see no extra round trips; small enough that all connections
   * of a pool fetching at once hold little compared to a typical heap (10 connections: ~80 MB).
   */
  val DefaultTargetBytes: Long = 8L * 1024 * 1024

  def fetchSizeFor(targetBytes: Long, largestRowBytes: Long, maxFetchSize: Int): Int =
    math.max(1L, math.min(maxFetchSize.toLong, targetBytes / math.max(1L, largestRowBytes))).toInt

}
