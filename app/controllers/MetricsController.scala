package controllers

import java.io._

import com.monsanto.arch.kamon.prometheus.metric.{ProtoBufFormat, TextFormat, MetricFamily}
import com.monsanto.arch.kamon.prometheus.{PrometheusListener, Prometheus}
import io.prometheus.client.CollectorRegistry
import kamon.Kamon
import akka.pattern.ask
import play.api.mvc.{RequestHeader, Accepting, Action, Controller}

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

object MetricsController extends Controller {

  type Formatter = (Seq[MetricFamily]) => (Array[Byte], String)

  val AcceptsTextPlain = Accepting("text/plain")
  val AcceptsProtoBuf = Accepting("application/vnd.google.protobuf")

  val textContentType = "text/plain; version=0.0.4"
  val protoBufContentType = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"

  lazy val textFormatter : Formatter = s => (TextFormat.format(s).getBytes(), textContentType)
  lazy val protoBufFormatter: Formatter = s => (ProtoBufFormat.format(s), protoBufContentType)
  lazy val listener = Kamon( Prometheus ).listener

  implicit val timeout : akka.util.Timeout = 5.seconds

  def get() = Action.async { implicit req => {

    (for {
        snapshot <- (listener ? PrometheusListener.Read).mapTo[Seq[MetricFamily]]
        formatter = choseFormatter
        (bytes, contentType) = formatter(snapshot)
        withPrometheusSamples = writePrometheusSamples()
        _ = println(s"snapshot: ${new String(bytes, "UTF-8").size}")
        _ = println(s"prometh: ${new String(withPrometheusSamples, "UTF-8").size}")
      } yield {
        Ok( bytes ++ withPrometheusSamples ).withHeaders("Content-type" -> contentType)
      }) recover {
          case t : Throwable => BadRequest(s"${t.getMessage}")
      }
    }
  }

  private def writePrometheusSamples() : Array[Byte]  = {
    val out = new ByteArrayOutputStream
    import io.prometheus.client.exporter.common.TextFormat
    val samples = CollectorRegistry.defaultRegistry.metricFamilySamples()
    val writer = new BufferedWriter( new OutputStreamWriter(out, "UTF-8"))
    TextFormat.write004(writer, samples)
    writer.flush()
    out.toByteArray
  }

  private def choseFormatter(implicit req: RequestHeader) : Formatter = req match {
    case AcceptsTextPlain() => textFormatter
//    case AcceptsProtoBuf() => protoBufFormatter
    case _ => throw new RuntimeException(s"No accepted media type : ${req.acceptedTypes}")
  }

}


