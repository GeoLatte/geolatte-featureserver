package metrics

import akka.actor.Actor
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot

/**
  * A SimplePrinter
  * Created by Karel Maesen, Geovise BVBA on 08/02/16.
  */
class SimplePrinter extends Actor {
  def receive = {
    case tickSnapshot: TickMetricSnapshot =>
      val requestMetrics = tickSnapshot.metrics.filterKeys( _.category == "request" )
      val histograms = tickSnapshot.metrics.filterKeys( _.category == "histogram" )

      println( "#################################################" )
      println( "From: " + tickSnapshot.from )
      println( "To: " + tickSnapshot.to )

      println( tickSnapshot )
  }

}