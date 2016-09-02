package controllers

import java.io._


import io.prometheus.client.CollectorRegistry
import play.api.mvc._

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext


object MetricsController extends Controller {

  val AcceptsTextPlain = Accepting("text/plain")
  val AcceptsProtoBuf = Accepting("application/vnd.google.protobuf")

  val textContentType = "text/plain; version=0.0.4"
  val protoBufContentType = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"


  implicit val timeout : akka.util.Timeout = 5.seconds

  def get() = Action  { implicit req => {
    val (bytes, contentType) = writeText
        Ok( bytes ).withHeaders("Content-type" -> contentType)
    }
  }

  private def writeText() : (Array[Byte], String)  = {
    val out = new ByteArrayOutputStream
    import io.prometheus.client.exporter.common.TextFormat
    val samples = CollectorRegistry.defaultRegistry.metricFamilySamples()
    val writer = new BufferedWriter( new OutputStreamWriter(out, "UTF-8"))
    TextFormat.write004(writer, samples)
    writer.flush()
    (out.toByteArray, TextFormat.CONTENT_TYPE_004)
  }

}


