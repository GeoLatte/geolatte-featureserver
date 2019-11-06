package integration

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.{ ByteString, Timeout }
import config.Constants
import controllers.SupportedMediaTypes
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.curve.{ MortonCode, MortonContext }
import org.specs2.matcher._
import play.api.{ Application, Logger }
import play.api.http._
import play.api.inject.guice.{ GuiceApplicationBuilder, GuiceApplicationLoader }
import play.api.libs.json._
import play.api.mvc._
import play.api.test._
import utilities.Utils.withInfo

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.language.implicitConversions

/**
 * Test Helper for Requests.
 *
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/12/13
 * @tparam B Type for request body as passed into test requests
 * @tparam T Type of content for request body, passed to test server
 * @tparam R Type of result body content
 */
case class FakeRequestResult[B, T, R](
  url:         String,
  format:      Option[Future[Result] => R]           = None,
  requestBody: Option[B]                             = None,
  mkRequest:   (String, Option[B]) => FakeRequest[T]
)(
  implicit
  val w:   Writeable[T],
  val app: Application
) {

  implicit val mat = app.materializer

  import UtilityMethods._

  val wrappedResult: Future[Result] = {
    val req = mkRequest(url, requestBody)
    route(app, req) match {
      case Some(res) => withInfo(s"Result for $url: $res") {
        res
      }
      case None => throw new RuntimeException("Route failed to execute for req: " + req)
    }
  }
  /**
   * Calling status on a FakeRequest forces a wait for the result.
   */
  val status: Int = play.api.test.Helpers.status(wrappedResult)

  val responseBody = format.map(f => f(wrappedResult))

  def applyMatcher[M](matcher: FakeRequestResult[B, T, R] => MatchResult[M]) = matcher(this)

}

object FakeRequestResult {

  import UtilityMethods._

  import scala.reflect.runtime.universe._

  def GET[T: TypeTag](url: String, format: Future[Result] => T)(implicit app: Application): FakeRequestResult[Nothing, AnyContentAsEmpty.type, T] =
    format match {
      case fmt if typeOf[T] <:< typeOf[JsValue] =>
        val mediaType = Constants.Format.JSON
        new FakeRequestResult(url = url, format = Some(format), mkRequest = makeGetRequest(mediaType))

      case fmt if typeOf[T] <:< typeOf[Seq[String]] =>
        val mediaType = Constants.Format.CSV
        new FakeRequestResult(url = url, format = Some(format), mkRequest = makeGetRequest(mediaType))

    }

  def PUT(url: String, body: Option[JsValue] = None)(implicit app: Application) =
    FakeRequestResult(url = url, requestBody = body, mkRequest = makePutRequest)

  def DELETE(url: String)(implicit app: Application) =
    FakeRequestResult[JsValue, AnyContentAsEmpty.type, JsValue](url = url, mkRequest = makeDeleteRequest)

  def POSTJson(url: String, body: JsValue, format: Future[Result] => JsValue)(implicit app: Application) =
    FakeRequestResult(url = url, format = Some(format), requestBody = Some(body), mkRequest = makePostRequestJson)

  def POSTRaw(url: String, body: ByteString, format: Future[Result] => JsValue)(implicit app: Application) =
    FakeRequestResult(url = url, format = Some(format), requestBody = Some(body), mkRequest = makePostRequestRaw)

}

//This is for pattern match
object ResultCheck {

  def unapply[B, T, R](fr: FakeRequestResult[B, T, R]): Option[(Int, Option[R])] = Some(fr.status, fr.responseBody)
}

trait RestApiDriver {

  this: FeatureServerSpecification =>

  import API._
  import UtilityMethods._
  import utilities.Utils.Logger

  import scala.reflect.runtime.universe._

  def makeDatabase(dbName: String) = {
    Logger.info("START CREATING DATABASE OR SCHEMA")
    val url = DATABASES / dbName
    FakeRequestResult.PUT(url)
  }

  def dropDatabase(dbName: String) = {
    FakeRequestResult.DELETE(DATABASES / dbName)
  }

  def getDatabases = {
    val format: Future[Result] => JsValue = (resp: Future[Result]) => contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _               => JsNull //indicates that something wrong
    }
    FakeRequestResult.GET(DATABASES, format)

  }

  def getDatabase(dbName: String) = {
    val format = (resp: Future[Result]) => contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _               => JsNull //indicates that something wrong
    }
    FakeRequestResult.GET(DATABASES / dbName, format)
  }

  def makeCollection(dbName: String, colName: String, metadata: JsObject = defaultCollectionMetadata) = {
    Logger.info("START CREATING COLLECTION")
    val url = DATABASES / dbName / colName
    FakeRequestResult.PUT(url, Some(metadata))
  }

  def getCollection(dbName: String, colName: String) = {
    Logger.info("Start GETTING Collection")
    val url = DATABASES / dbName / colName
    FakeRequestResult.GET(url, contentAsJson)
  }

  def deleteCollection(dbName: String, colName: String) = {
    Logger.info("START DELETING Collection")
    val url = DATABASES / dbName / colName
    FakeRequestResult.DELETE(url)
  }

  def getQuery[T: TypeTag](dbName: String, colName: String, queryStr: String)(format: Future[Result] => T) = {
    Logger.info("Start Collection Query with QUERY parameter: " + queryStr)
    val url = DATABASES / dbName / colName / QUERY ? queryStr
    FakeRequestResult.GET(url, format)
  }

  def getDistinct[T: TypeTag](dbName: String, colName: String, queryStr: String)(format: Future[Result] => T) = {
    Logger.info("Start Collection Distinct with QUERY parameter: " + queryStr)
    val url = DATABASES / dbName / colName / DISTINCT ? queryStr
    FakeRequestResult.GET(url, format)
  }

  def getList(dbName: String, colName: String, queryStr: String) = {
    Logger.info("Start /list on collection")
    val url = DATABASES / dbName / colName / FEATURECOLLECTION ? queryStr
    FakeRequestResult.GET(url, contentAsJsonStringStream)
  }

  def postUpsert(dbName: String, colName: String, reqBody: JsObject) = {
    Logger.info("START UPSERT COLLECTION")
    val url = DATABASES / dbName / colName / TX / UPSERT
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postInsert(dbName: String, colName: String, reqBody: JsObject) = {
    Logger.info("START INSERT COLLECTION")
    val url = DATABASES / dbName / colName / TX / INSERT
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postUpdate(dbName: String, colName: String, reqBody: JsObject) = {
    Logger.info("START UPDATE COLLECTION")
    val url = DATABASES / dbName / colName / TX / UPDATE
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postRemove(db: String, col: String, reqBody: JsObject) = {
    Logger.info(s"START REMOVING DATA WITH REQUEST ${Json.stringify(reqBody)}")
    val url = DATABASES / db / col / TX / REMOVE
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postMediaObject(dbName: String, colName: String, mediaObject: JsObject) = {
    val url = DATABASES / dbName / colName / MEDIA

    val format = (posted: Future[Result]) => contentAsJson(posted) match {
      case obj: JsObject => obj
      case _             => JsNull
    }
    FakeRequestResult.POSTJson(url, mediaObject, format)
  }

  def getMediaObject(url: String) = {
    val format = (resp: Future[Result]) => contentAsJson(resp) match {
      case obj: JsObject => obj
      case _             => JsNull //indicates that something wrong
    }
    FakeRequestResult.GET(url, format)
  }

  def deleteMediaObject(url: String) = {
    FakeRequestResult.DELETE(url)
  }

  def putView(dbName: String, colName: String, viewName: String, body: JsObject) = {
    val url = DATABASES / dbName / colName / VIEWS / viewName
    FakeRequestResult.PUT(url, Some(body))
  }

  def getViews(dbName: String, colName: String) = {
    val url = DATABASES / dbName / colName / VIEWS
    FakeRequestResult.GET(url, contentAsJson)
  }

  def getView(dbName: String, colName: String, id: String) = {
    val url = DATABASES / dbName / colName / VIEWS / id
    FakeRequestResult.GET(url, contentAsJson)
  }

  def deleteView(dbName: String, colName: String, id: String) = {
    val url = DATABASES / dbName / colName / VIEWS / id
    FakeRequestResult.DELETE(url)
  }

  def putIndex(dbName: String, colName: String, idx: String, body: JsObject) = {
    val url = DATABASES / dbName / colName / INDEXES / idx
    FakeRequestResult.PUT(url, Some(body))
  }

  def getIndices(dbName: String, colName: String) = {
    val url = DATABASES / dbName / colName / INDEXES
    FakeRequestResult.GET(url, contentAsJson)
  }

  def getIndex(dbName: String, colName: String, idx: String) = {
    val url = DATABASES / dbName / colName / INDEXES / idx
    FakeRequestResult.GET(url, contentAsJson)
  }

  def deleteIndex(dbName: String, colName: String, idx: String) = {
    val url = DATABASES / dbName / colName / INDEXES / idx
    FakeRequestResult.DELETE(url)
  }

  def loadData[B](db: String, col: String, data: ByteString) = {
    Logger.info("START LOADING TEST DATA")
    val url = DATABASES / db / col / TX / INSERT
    val res = FakeRequestResult.POSTRaw(url, data, contentAsJson)
    Logger.info("LOADED TEST DATA")
    Logger.info("Loader status " + res.status)
    res
  }

  def upsertData[B](db: String, col: String, data: ByteString) = {
    Logger.info("START LOADING TEST DATA")
    val url = DATABASES / db / col / TX / UPSERT
    val res = FakeRequestResult.POSTRaw(url, data, contentAsJson)
    Logger.info("UPSERTED TEST DATA")
    Logger.info("Loader status " + res.status)
    res
  }

  def loadView(dbName: String, colName: String, viewName: String, body: JsObject) = {
    Logger.info("START LOADING TEST VIEW " + viewName)
    val url = DATABASES / dbName / colName / VIEWS / viewName
    FakeRequestResult.PUT(url, Some(body)).status
  }

  def removeData(db: String, col: String) = {
    Logger.info("START REMOVING TEST DATA")
    val url = DATABASES / db / col / TX / REMOVE
    FakeRequestResult.POSTJson(url, Json.obj("query" -> "TRUE"), res => JsNull)
    Logger.info("REMOVED TEST DATA")
  }

  def withFeatures[B, T](db: String, col: String, features: JsArray)(block: => T) = {
    val data = features.value map (j => Json.stringify(j)) mkString Constants.chunkSeparator getBytes "UTF-8"
    loadData(db, col, ByteString(data))
    try {
      block
    } finally {
      removeData(db, col)
    }
  }

}

object UtilityMethods extends PlayRunners
  with HeaderNames
  with Status
  with HttpProtocol
  with DefaultAwaitTimeout
  with ResultExtractors
  with Writeables
  with RouteInvokers
  //with WsTestClient
  with FutureAwaits {

  override implicit def defaultAwaitTimeout = 60.seconds

  //  implicit val actorSys = ActorSystem()
  //  implicit val mat = ActorMaterializer()

  val defaultIndexLevel = 4
  implicit val defaultExtent = new Envelope(0, 0, 90, 90, CrsId.valueOf(4326))
  implicit val defaultMortonCode = new MortonCode(new MortonContext(defaultExtent, defaultIndexLevel))

  val defaultCollectionMetadata = Json.obj(
    "extent" -> Json.obj("crs" -> defaultExtent.getCrsId.getCode, "envelope" ->
      Json.arr(defaultExtent.getMinX, defaultExtent.getMinY, defaultExtent.getMaxX, defaultExtent.getMaxY)),
    "index-level" -> defaultIndexLevel,
    "id-type" -> "decimal"
  )

  implicit def mapOfQParams2QueryStr[T](params: Map[String, T]): String = params.map { case (k, v) => s"$k=$v" } mkString "&"

  def makeGetRequest[B](mediatype: Constants.Format.Value)(url: String, js: Option[B] = None) =
    FakeRequest(GET, url).withHeaders("Accept" -> SupportedMediaTypes(mediatype).toString)

  def makePutRequest(url: String, body: Option[JsValue] = None) = FakeRequest(PUT, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestJson(url: String, body: Option[JsValue]) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestRaw(url: String, body: Option[ByteString]) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withRawBody(body.getOrElse(ByteString.empty))

  def makeDeleteRequest(url: String, body: Option[JsValue] = None) = FakeRequest(DELETE, url)

  //when formatting the resonsebody for purposes of interpreting response bodies in test, we
  // transform pure text strings into JsString objects.
  private def parseJson(responseText: String): JsValue =
    try {
      Json.parse(responseText)
    } catch {
      case ex: Throwable => JsString(responseText)
    }

  override def contentAsJson(result: Future[Result])(implicit timeout: Timeout, mat: Materializer): JsValue = {
    val responseText = contentAsString(result)(timeout, mat)
    parseJson(responseText)
  }

  def extractSizeAndData(bytes: ByteString): String = {
    bytes.decodeString("UTF8")
  }

  private def readChunked[T](result: Future[Result], parse: String => T)(timeout: Timeout)(implicit mat: Materializer): Seq[T] = {
    val buffer = ListBuffer[T]()

    val body = Await.result(result, timeout.duration).body.dataStream
    val sink: Sink[ByteString, Future[scala.collection.Seq[T]]] = Sink.fold[ListBuffer[T], ByteString](buffer) { (buf, bytes) =>
      {
        val text = extractSizeAndData(bytes)
        try {
          buf += parse(text)
        } catch {
          case _: Throwable => buf
        }
      }
    }

    val f = body.runWith(sink)
    Await.result(f, timeout.duration).toSeq
  }

  def isTransferEncoded(fresult: Future[Result])(implicit timeout: Timeout): Boolean = {
    val b: HttpEntity = Await.result(fresult, timeout.duration).body
    b.isInstanceOf[HttpEntity.Chunked] || b.isInstanceOf[HttpEntity.Streamed]
  }

  def contentAsJsonStringStream(result: Future[Result])(implicit timeout: Timeout, mat: Materializer): JsObject =
    isTransferEncoded(result)(timeout) match {
      case true => Json.parse(
        readChunked(result, s => s)(timeout).foldLeft("")((s, result) => s + result)
      ).asInstanceOf[JsObject]
      case _ => Json.parse(contentAsString(result)(timeout, mat)).asInstanceOf[JsObject]
    }

  def contentAsJsonStream(result: Future[Result])(implicit timeout: Timeout, mat: Materializer): JsArray =
    isTransferEncoded(result)(timeout) match {
      case true => JsArray(readChunked(result, Json.parse)(timeout))
      case _    => Json.arr(contentAsJson(result)(timeout, mat))
    }

  def contentAsStringStream(result: Future[Result])(implicit timeout: Timeout, mat: Materializer): Seq[String] =
    isTransferEncoded(result)(timeout) match {
      case true => readChunked(result, identity[String])(timeout)
      case _    => Seq(contentAsString(result)(timeout, mat))
    }

  implicit def toArrayOpt(js: Option[JsValue]): Option[JsArray] = js match {
    case Some(ja: JsArray) => Some(ja)
    case _                 => None
  }

  implicit def fakeRequestToResult[B, T, R](fake: FakeRequestResult[B, T, R]): Future[Result] = fake.wrappedResult

}

object API {

  case class URLToken(token: String) {

    def /(other: URLToken): URLToken = URLToken(s"$token/${other.token}")

    def ?(other: URLToken): URLToken = URLToken(s"$token?${other.token}")
  }

  implicit def URLToken2String(urlToken: URLToken): String = urlToken.token

  implicit def String2URLToken(str: String): URLToken = URLToken(str)

  val DATABASES = "/api/databases"

  val TX = "tx"

  val DOWNLOAD = "download"

  val QUERY = "query"

  val DISTINCT = "distinct"

  val FEATURECOLLECTION = "featurecollection"

  val INSERT = "insert"

  val REMOVE = "remove"

  val UPDATE = "update"

  val UPSERT = "upsert"

  val MEDIA = "media"

  val VIEWS = "views"

  val INDEXES = "indexes"

}
