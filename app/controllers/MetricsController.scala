package controllers

import java.io._

import com.monsanto.arch.kamon.prometheus.Prometheus
import com.monsanto.arch.kamon.prometheus.PrometheusExtension._
import akka.pattern.ask
import com.monsanto.arch.kamon.prometheus.metric.{MetricFamily, TextFormat => KamonPrometheusTextFormat}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.{TextFormat => PrometheusTextFormat}
import nosql.FutureInstrumented
import nosql.Utils._
import play.api.mvc._

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import scala.concurrent.Future


object MetricsController extends Controller with FutureInstrumented{

  val AcceptsTextPlain = Accepting("text/plain")
  val AcceptsProtoBuf = Accepting("application/vnd.google.protobuf")

  val textContentType = "text/plain; version=0.0.4"
  val protoBufContentType = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"


  implicit val timeout: akka.util.Timeout = 5.seconds


  def getKamonSamples: Future[String] = {

    val fSnapshot = for {
      extension <- Prometheus.kamonInstance
      Snapshot(s) <- extension.ref.ask(GetCurrentSnapshot)(1 second)
    } yield s

    fSnapshot
      .map {
        KamonPrometheusTextFormat.format(_)
      }.recoverWith{case ex =>
        withWarning(s"No Kamon metrics snapshot, because $ex") {
          Future.successful("")
        }
      }
  }


  def get() = Action.async { implicit req =>
    futureTimed("metrics-response-time"){
      for {
        kamonMetrics <- getKamonSamples
        prometheusMetrics = getPrometheusSamples
      } yield Ok(prometheusMetrics + "\n" + kamonMetrics).withHeaders("Content-type" -> PrometheusTextFormat.CONTENT_TYPE_004)
    }
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


