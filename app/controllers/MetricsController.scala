package controllers

import com.monsanto.arch.kamon.prometheus.metric.{ProtoBufFormat, TextFormat, MetricFamily}
import com.monsanto.arch.kamon.prometheus.{PrometheusListener, Prometheus}
import kamon.Kamon
import akka.pattern.ask
import play.api.mvc.{RequestHeader, Accepting, Action, Controller}

import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext


object MetricsController extends Controller {

  type Formatter = (Seq[MetricFamily]) => Array[Byte]

  val AcceptsTextPlain = Accepting("text/plain")
  val AcceptsProtoBuf = Accepting("application/vnd.google.protobuf")

  lazy val textFormatter : Formatter = s => TextFormat.format(s).getBytes()
  lazy val protoBufFormatter: Formatter = s => ProtoBufFormat.format(s)
  lazy val listener = Kamon( Prometheus ).listener

  implicit val timeout : akka.util.Timeout = 5.seconds

  def get() = Action.async { implicit req => {
    (for {
        snapshot <- (listener ? PrometheusListener.Read).mapTo[Seq[MetricFamily]]
        formatter = choseFormatter
        bytes = formatter(snapshot)
      } yield {
        Ok(bytes)
      }) recover {
          case t : Throwable => BadRequest(s"${t.getMessage}")
      }
    }
  }

  private def choseFormatter(implicit req: RequestHeader) : Formatter = req match {
    case AcceptsProtoBuf() => protoBufFormatter
    case AcceptsTextPlain() => textFormatter
    case _ => throw new RuntimeException(s"No accepted media type : ${req.acceptedTypes}")
  }

}


