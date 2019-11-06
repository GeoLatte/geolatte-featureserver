package metrics

import javax.inject.Inject

import persistence.SpatialQuery
import utilities.Utils.Logger

/**
 * Created by Karel Maesen, Geovise BVBA on 31/05/17.
 */

final case class Operation(name: String)

object Operation {

  val UPSERT = Operation("upsert")
  val DELETE = Operation("delete")
  val QUERY_COLLECTION = Operation("query_collection")
  val QUERY_STREAM = Operation("query_stream")
  val QUERY_DISTINCT = Operation("query_distinct")
  val CREATE_TABLE = Operation("create_table")
  val DROP_TABLE = Operation("drop_table")

}

trait Instrumentation {

  def incrementOperation(op: Operation, db: String, coll: String): Unit

  def updateSpatialQueryMetrics(db: String, coll: String, query: SpatialQuery): Unit
}

class StdInstrumentation @Inject() (implicit metrics: Metrics)
  extends Instrumentation {

  def incrementOperation(op: Operation, db: String, coll: String): Unit =
    try {
      metrics.prometheusMetrics.repoOperations.labels(op.name, db, coll).inc()
    } catch {
      case ex: Throwable => Logger.warn(s"Failure to update Ops Metrics", ex)
    }

  def updateSpatialQueryMetrics(db: String, coll: String, query: SpatialQuery): Unit =
    try {
      query.windowOpt.foreach { env =>
        val envDim = Math.hypot(env.getWidth, env.getHeight)
        metrics.prometheusMetrics.bboxHistogram.labels(db, coll).observe(envDim)
      }
    } catch {
      case ex: Throwable => Logger.warn(s"Failure to update SpatialQuery Metrics", ex)
    }

}
