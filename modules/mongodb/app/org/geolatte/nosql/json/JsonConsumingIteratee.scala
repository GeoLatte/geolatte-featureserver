package org.geolatte.nosql.json

import play.api.libs.iteratee.Iteratee
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import org.codehaus.jackson._
import org.geolatte.common.Feature
import scala.collection.immutable.VectorBuilder


/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/29/13
 */
trait JsonStreamingParser[T] {
  def parse(chunk: Array[Byte]): (this.type, Option[List[T]])
}

object ParserStates extends Enumeration {
  type ParserState = Value
  val START, FEATURES_PROPERTY_FOUND, IN_FEATURES_ARRAY, READ_FEATURE, END_FEATURES_ARRAY = Value
}


import ParserStates._

case class GeoJsonParserHelper(jp: JsonParser, initialState: ParserStates.Value = START) {

  val FEATURES_PROPERTY_NAME = "features"

  private var lastKnownGoodLocation = 0
  var currentState = initialState
  var currentToken: JsonToken = null
  var atEnd = false
  private val featureListBuilder = new VectorBuilder[Feature]

  override def toString = s"currentState: $currentState; currentToken: $currentToken; atEnd: $atEnd; currentLocation: ${currentLocation}, readVectors: ${featureListBuilder.result().size}"

  def transition(t: JsonToken) = t match {
    case JsonToken.FIELD_NAME if currentState == START && jp.getCurrentName.equals(FEATURES_PROPERTY_NAME) =>
        {currentState = FEATURES_PROPERTY_FOUND; true}
    case JsonToken.START_ARRAY if currentState == FEATURES_PROPERTY_FOUND =>
        {currentState = IN_FEATURES_ARRAY; true}
    case JsonToken.START_OBJECT if List(IN_FEATURES_ARRAY, READ_FEATURE).contains( currentState ) =>
         if(readFeature) { currentState = READ_FEATURE; true}
         else false
    case JsonToken.END_ARRAY if List(IN_FEATURES_ARRAY, READ_FEATURE).contains( currentState ) =>
        {currentState = END_FEATURES_ARRAY; true}
    case _ => false
  }

  def readFeature = {
    try {
      featureListBuilder += jp.readValueAs(classOf[Feature])
      currentToken = JsonToken.END_OBJECT
      lastKnownGoodLocation = jp.getCurrentLocation.getCharOffset.toInt + 1
      true
    } catch  {
      case _ : Throwable => {
        currentToken = null
        atEnd = true
        false
      }
    }
  }

  def next: this.type = {

    try {
      var t = jp.nextToken
      while (t != null && !transition(t)) {
        t = jp.nextToken
        lastKnownGoodLocation = jp.getCurrentLocation.getCharOffset.toInt
      }
      currentToken = t
      atEnd = t == null
    } catch {
      case _: Throwable => {
        currentToken = null
        atEnd = true
      }
    }
    this
  }

  def currentLocation = lastKnownGoodLocation

  def readFeatures: Vector[Feature] = featureListBuilder.result()

}

object JsonStreamingParser {

  lazy val codec = (new JsonMapper).getObjectMapper()
  lazy val jsonFactory = new JsonFactory(codec)

  def apply[T](): JsonStreamingParser[T] = apply(Array[Byte](), false)

  private def apply[T](base: Array[Byte], isReading: Boolean): JsonStreamingParser[T] = new JsonStreamingParser[T] {
    def parse(chunk: Array[Byte]) = {
      val jp = jsonFactory.createJsonParser(base ++ chunk)
      if (!isReading) {
        //skip parser to start of array of the "features collection"
      }
      (this, None)
    }
  }

}

object JsonConsumingIteratee {

  def newIteratee[T] = Iteratee.fold(JsonStreamingParser[T]()) {
    (parser: JsonStreamingParser[T], chunk: Array[Byte]) => parser.parse(chunk)._1
  }

}
