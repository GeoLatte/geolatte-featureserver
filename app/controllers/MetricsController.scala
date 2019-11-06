package controllers

import java.io._
import javax.inject.Inject

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.{ TextFormat => PrometheusTextFormat }
import persistence.Repository
import play.api.mvc._

import scala.concurrent.duration._

class MetricsController @Inject() (val repository: Repository) extends InjectedController {

  val AcceptsTextPlain = Accepting("text/plain")
  val AcceptsProtoBuf = Accepting("application/vnd.google.protobuf")

  val textContentType = "text/plain; version=0.0.4"
  val protoBufContentType = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"

  implicit val timeout: akka.util.Timeout = 5.seconds

  def get() = Action { implicit req =>
    val prometheusMetrics = getPrometheusSamples
    Ok(prometheusMetrics).withHeaders("Content-type" -> PrometheusTextFormat.CONTENT_TYPE_004)
  }

  private def getPrometheusSamples(): String = {
    val out = new ByteArrayOutputStream
    val samples = CollectorRegistry.defaultRegistry.metricFamilySamples()
    val writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))
    PrometheusTextFormat.write004(writer, samples)
    writer.flush()
    out.toString("UTF-8")
  }

}

