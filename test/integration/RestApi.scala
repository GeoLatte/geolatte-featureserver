package integration

import play.api.libs.json._
import play.api.libs.functional.syntax._

import play.api.test._
import play.api.mvc._
import utilities.SupportedMediaTypes

import language.implicitConversions
import play.api.Logger
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import play.api.http.{HttpProtocol, Status, HeaderNames, Writeable}
import org.specs2._
import org.specs2.matcher._
import config.ConfigurationValues
import org.geolatte.geom.curve.{MortonContext, MortonCode}
import scala.collection.mutable.ListBuffer
import akka.util.Timeout
import play.api.libs.iteratee.Iteratee



/**
 * Test Helper for Requests.
 *
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/12/13
 *
 * @tparam B Type for request body as passed into test requests
 * @tparam T Type of content for request body, passed to test server
 * @tparam R Type of result body content
 */
case class FakeRequestResult[B, T, R]( url: String,
                                       format: Option[Future[SimpleResult] => R] = None,
                                       requestBody: Option[B] = None,
                                       mkRequest: (String, Option[B]) => FakeRequest[T])(
                                       implicit val w: Writeable[T]
                                       ) {

  import UtilityMethods._

  val wrappedResult: Future[SimpleResult] = {
    val req = mkRequest(url, requestBody)
    route(req) match {
      case Some(res) => res
      case None => throw new RuntimeException("Route failed to execute for req: " + req)
    }
  }
  val status: Int = play.api.test.Helpers.status(wrappedResult)

  val responseBody = format.map(f => f(wrappedResult))

  def applyMatcher[M](matcher: FakeRequestResult[B, T, R] => MatchResult[M]) = matcher(this)

}

object FakeRequestResult {

  import UtilityMethods._

  import scala.reflect.runtime.universe._

  def GET[T : TypeTag](url: String, format: Future[SimpleResult] => T) : FakeRequestResult[Nothing, AnyContentAsEmpty.type, T]=
      format match {
        case fmt if typeOf[T]  <:< typeOf[JsValue] => {
          val mediaType = ConfigurationValues.Format.JSON
          new FakeRequestResult(url = url, format = Some(format), mkRequest = makeGetRequest(mediaType) )
        }
        case fmt if typeOf[T]  <:< typeOf[Seq[String]] => {
          val mediaType = ConfigurationValues.Format.CSV
          new FakeRequestResult(url = url, format = Some(format), mkRequest = makeGetRequest(mediaType))
        }
      }


  def PUT(url: String, body: Option[JsValue] = None) =
    new FakeRequestResult(url = url, requestBody = body, mkRequest = makePutRequest)

  def DELETE(url: String) = new FakeRequestResult[JsValue, AnyContentAsEmpty.type, JsValue](url = url, mkRequest = makeDeleteRequest)

  def POSTJson(url: String, body: JsValue, format: Future[SimpleResult] => JsValue) =
    new FakeRequestResult(url = url, format = Some(format), requestBody = Some(body), mkRequest = makePostRequestJson)

  def POSTRaw(url: String, body: Array[Byte], format: Future[SimpleResult] => JsValue) =
    new FakeRequestResult(url = url, format = Some(format), requestBody = Some(body), mkRequest = makePostRequestRaw)

}

//This is for pattern match
object ResultCheck {
  def unapply[B, T, R](fr: FakeRequestResult[B, T, R]): Option[(Int, Option[R])] = Some(fr.status, fr.responseBody)
}


object RestApiDriver {

  import UtilityMethods._
  import API._
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
    val format : Future[SimpleResult] => JsValue = (resp: Future[SimpleResult]) => contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _ => JsNull //indicates that something wrong
    }
    FakeRequestResult.GET(DATABASES, format)

  }

  def getDatabase(dbName: String) = {
    val format = (resp: Future[SimpleResult]) => contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _ => JsNull //indicates that something wrong
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
    val url = DATABASES/ dbName / colName
    FakeRequestResult.DELETE(url)
  }

  def getDownload(dbName: String, colName: String) = {
    Logger.info("Start download Collection")
    val url = DATABASES / dbName / colName / DOWNLOAD
    FakeRequestResult.GET(url, contentAsJsonStream)
  }

  def getQuery[T : TypeTag](dbName: String, colName: String, queryStr: String)(format : Future[SimpleResult] => T) = {
    Logger.info("Start Collection Query")
    val url = DATABASES / dbName / colName / QUERY ? queryStr
    FakeRequestResult.GET(url, format)
  }

  def getList(dbName: String, colName: String, queryStr: String) = {
    Logger.info("Start /list on collection")
    val url = DATABASES / dbName / colName / FEATURECOLLECTION ? queryStr
    FakeRequestResult.GET(url, contentAsJson)
  }

  def postUpsert(dbName: String, colName: String, reqBody: JsObject) = {
    Logger.info("START UPSERT COLLECTION")
    val url = DATABASES / dbName / colName / TX / UPSERT
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postUpdate(dbName: String, colName: String, reqBody: JsObject) = {
    Logger.info("START UPDATE COLLECTION")
    val url = DATABASES / dbName / colName / TX / UPSERT
    FakeRequestResult.POSTJson(url, reqBody, contentAsJson)
  }

  def postMediaObject(dbName: String, colName: String, mediaObject: JsObject) = {
    val url = DATABASES / dbName / colName / MEDIA

    val format = (posted: Future[SimpleResult]) => contentAsJson(posted) match {
      case obj: JsObject => obj
      case _ => JsNull
    }
    FakeRequestResult.POSTJson(url, mediaObject, format)
  }

  def getMediaObject(url: String) = {
    val format = (resp: Future[SimpleResult]) => contentAsJson(resp) match {
      case obj: JsObject => obj
      case _ => JsNull //indicates that something wrong
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

  def loadData[B](db: String, col: String, data: Array[Byte]) = {
    Logger.info("START LOADING TEST DATA")
    val url = DATABASES / db / col / TX / INSERT
    val res = FakeRequestResult.POSTRaw(url, data, contentAsJson)
    Logger.info("LOADED TEST DATA")
    res
  }

  def removeData(db: String, col: String) = {
    Logger.info("START REMOVING TEST DATA")
    val url = DATABASES / db / col / TX / REMOVE
    FakeRequestResult.POSTJson(url, Json.obj("query" -> Json.obj()), res => JsNull)
    Logger.info("REMOVED TEST DATA")
  }

  def withFeatures[B, T](db: String, col: String, features: JsArray)(block: => T) = {
    val data = features.value map (j => Json.stringify(j)) mkString ConfigurationValues.jsonSeparator getBytes ("UTF-8")
    loadData(db, col, data)
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
with WsTestClient
with FutureAwaits {

  import play.api.libs.concurrent._

  override implicit def defaultAwaitTimeout = 60.seconds

  val defaultIndexLevel = 4
  implicit val defaultExtent = new Envelope(0, 0, 90, 90, CrsId.valueOf(4326))
  implicit val defaultMortonCode = new MortonCode(new MortonContext(defaultExtent, defaultIndexLevel))

  val defaultCollectionMetadata = Json.obj(
    "extent" -> Json.obj("crs" -> defaultExtent.getCrsId.getCode, "envelope" ->
      Json.arr(defaultExtent.getMinX, defaultExtent.getMinY, defaultExtent.getMaxX, defaultExtent.getMaxY)),
    "index-level" -> defaultIndexLevel
  )

  implicit def mapOfQParams2QueryStr[T](params: Map[String, T]): String = params.map { case (k, v) => s"$k=$v"} mkString "&"

  def makeGetRequest[B](mediatype: ConfigurationValues.Format.Value)(url: String, js: Option[B] = None) =
    FakeRequest(GET, url).withHeaders("Accept" -> SupportedMediaTypes(mediatype).toString)


  def makePutRequest(url: String, body: Option[JsValue] = None) = FakeRequest(PUT, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestJson(url: String, body: Option[JsValue]) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestRaw(url: String, body: Option[Array[Byte]]) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withRawBody(body.getOrElse(Array[Byte]()))

  def makeDeleteRequest(url: String, body: Option[JsValue] = None) = FakeRequest(DELETE, url)

  private def parseJson(responseText: String): JsValue =
    try {
      Json.parse(responseText)
    } catch {
      case ex: Throwable => {
        if (!responseText.isEmpty) Logger.warn(s"Error on parsing text: $responseText \nError-message is: ${ex.getMessage}")
        JsNull
      }
    }

  override def contentAsJson(result: Future[SimpleResult])(implicit timeout: Timeout): JsValue = {
    val responseText = contentAsString(result)(timeout)
    parseJson(responseText)
  }

  def extractSizeAndData(bytes: Array[Byte]): (Int, String) = {
    val elems = new String(bytes, "UTF-8").split("\r\n")
    val chunkSize = Integer.parseInt(elems(0), 16)
    if (elems.size == 1) (chunkSize, "")
    else (chunkSize, elems(1))

  }

  private def readChunked[T](result: Future[SimpleResult], parse: String => T)(timeout: Timeout): Seq[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val buf = ListBuffer[T]()
    val consumer: Iteratee[Array[Byte], Unit] =
      Iteratee.fold[Array[Byte], ListBuffer[T]](buf)((buf, bytes) => {
        val (size, text) = extractSizeAndData(bytes)
        try {
          buf += parse(text)
        } catch {
          case _: Throwable => buf
        }
      }).map(_ => Unit)
    val body = Await.result(result, timeout.duration).body
    val f = (body |>>> consumer)
    Await.result(f, timeout.duration)
    buf.toSeq
  }

  def contentAsJsonStream(result: Future[SimpleResult])(implicit timeout: Timeout): JsArray =
    header("Transfer-Encoding", result)(timeout) match {
      case Some("chunked") => JsArray(readChunked(result, Json.parse)(timeout))
      case _ => Json.arr(contentAsJson(result)(timeout))
    }

  def contentAsStringStream(result: Future[SimpleResult])(implicit timeout: Timeout): Seq[String] =
    header("Transfer-Encoding", result)(timeout) match {
      case Some("chunked") => readChunked(result, identity[String])(timeout)
      case _ => Seq(contentAsString(result)(timeout))
    }


  implicit def toArrayOpt(js: Option[JsValue]): Option[JsArray] = js match {
    case Some(ja: JsArray) => Some(ja)
    case _ => None
  }

  implicit def fakeRequestToResult[B, T, R](fake: FakeRequestResult[B, T, R]): Future[SimpleResult] = fake.wrappedResult

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

  val FEATURECOLLECTION = "featurecollection"

  val INSERT = "insert"

  val REMOVE = "remove"

  val UPDATE = "update"

  val UPSERT = "upsert"

  val MEDIA = "media"

  val VIEWS = "views"

}