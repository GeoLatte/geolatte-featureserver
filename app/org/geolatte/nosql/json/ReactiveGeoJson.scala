package org.geolatte.nosql.json

import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee._
import org.geolatte.common.Feature
import play.api.mvc.{Results, Result, BodyParser}
import java.io._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import org.codehaus.jackson.{JsonParser, JsonToken, JsonFactory}
import scala.collection.immutable.VectorBuilder


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
object ReactiveGeoJson {

  // case class that we will fold the result of the parsing into
  case class State(msg: String = "", errors: List[String])

  object ParserStates extends Enumeration {
    type ParserState = Value
    val START, CRS_PROPERTY, FEATURES_PROPERTY, IN_FEATURES_ARRAY, START_FEATURE, END_FEATURES_ARRAY, EOF = Value
  }

  trait JsonStreamingParser {

    import ParserStates._

    val FEATURES_PROPERTY_NAME = "features"
    val CRS_PROPERTY_NAME = "crs"

    def jp: JsonParser

    var currentState: ParserStates.Value = START

    def featureWriter: FeatureWriter

    private def transition(t: JsonToken) = t match {
      case JsonToken.FIELD_NAME if List(START).contains(currentState) && jp.getCurrentName.equals(CRS_PROPERTY_NAME) => {
        currentState = CRS_PROPERTY;
        true
      }
      case JsonToken.FIELD_NAME if List(START, CRS_PROPERTY).contains(currentState) && jp.getCurrentName.equals(FEATURES_PROPERTY_NAME) => {
        currentState = FEATURES_PROPERTY;
        true
      }
      case JsonToken.START_ARRAY if currentState == FEATURES_PROPERTY => {
        currentState = IN_FEATURES_ARRAY;
        true
      }
      case JsonToken.START_OBJECT if List(IN_FEATURES_ARRAY, START_FEATURE).contains(currentState) => {
        currentState = START_FEATURE;
        true
      }
      case JsonToken.END_ARRAY if List(IN_FEATURES_ARRAY, START_FEATURE).contains(currentState) => {
        currentState = END_FEATURES_ARRAY;
        true
      }
      case _ => false
    }

    def next: ParserStates.Value = {
      var t = jp.nextToken
      while (t != null && !transition(t)) {
        t = jp.nextToken
      }
      if (t == null) EOF
      else currentState
    }

    def readFeature = try {
      Right(jp.readValueAs(classOf[Feature]))
    } catch {
      case ex: Throwable => Left(ex.getMessage)
    }

    def readCRS = try {
      jp.nextToken
      CrsId.valueOf(jp.getIntValue)
    } catch {
      case ex: Throwable => CrsId.UNDEFINED //TODO -- collect errors in State object
    }

    def run: State = {
      var cnt = 0
      var errs = 0
      var crs = CrsId.UNDEFINED
      val errMsgBuilder = new VectorBuilder[String]
      while (next != EOF) {
        if (currentState == CRS_PROPERTY) crs = readCRS
        if (currentState == START_FEATURE) {
          readFeature match {
            case Right(f) => if (featureWriter.add(f)) cnt += 1
                             else errMsgBuilder += ("Writer did not accept feature with envelope: %s" format f.getGeometry.getEnvelope.toString)
            case Left(ex) => errMsgBuilder += ex
          }
        }
      }
      val msgs = errMsgBuilder.result().toList
      State("%d features read successfully; %d features failed.\n" format(cnt,msgs.size), msgs)
    }

  }

  object JsonStreamingParser {

    lazy val codec = (new JsonMapper).getObjectMapper
    lazy val jsonFactory = new JsonFactory(codec)

    def apply[T](in: InputStream, writer: FeatureWriter) = new JsonStreamingParser {
      lazy val jp = jsonFactory.createJsonParser(in)
      val featureWriter = writer
    }

  }

  def mkStreamingIteratee(writer: FeatureWriter)(implicit ec: ExecutionContext): Iteratee[Array[Byte], Either[Result, State]] = {
    val pOutput = new PipedOutputStream()
    val pInput = new PipedInputStream(pOutput)

    val worker = Future {
      val reader = JsonStreamingParser(pInput, writer)
      reader.run
    }

    Iteratee.fold[Array[Byte], PipedOutputStream](pOutput) {
      (pOutput, data) =>
        pOutput.write(data)
        pOutput
    }.mapDone {
      pOutput =>
        pOutput.close()
        try {
          val s = Await.result(worker, Duration(60, "sec"))
          Right(s)
        } catch {
          case t: Throwable => Left(Results.InternalServerError(t.getMessage))
        }
    }
  }

  def bodyParser(writer: FeatureWriter)(implicit ec: ExecutionContext) = BodyParser(rh => mkStreamingIteratee(writer))


}
