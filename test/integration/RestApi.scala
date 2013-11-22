package integration

import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import play.api.mvc.{ChunkedResult, PlainResult, AsyncResult, Result}

import language.implicitConversions
import play.api.Logger
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.Json.JsValueWrapper
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/12/13
 */

trait OpResult {
  def wrappedResult: Result

  def status: Int

  def url: String
}

case class GETResult(url: String, status: Int, responseBody: JsValue = JsNull, wrappedResult: Result) extends OpResult

case class PUTResult(url: String, status: Int, requestBody: JsValue = JsNull, wrappedResult: Result) extends OpResult

case class POSTResult(url: String, status: Int, requestBody: JsValue = JsNull, responseBody: JsValue = JsNull,
                      wrappedResult: Result) extends OpResult

case class DELETEResult(url: String, status: Int, wrappedResult: Result) extends OpResult


object RestApiDriver {

  import UtilityMethods._
  import API._

  def makeDatabase(dbName: String): PUTResult = {
    Logger.info("START CREATING DATABASE")
    val url = DATABASES/dbName
    val put = makePutRequest(url, JsNull)
    val created = route(put).get
    val st = status(created)
    Logger.info("CREATED DATABASE")
    PUTResult(url, st, wrappedResult = created)
  }

  def dropDatabase(dbName: String): DELETEResult = {
    val url = DATABASES/dbName
    val dropReq = makeDeleteRequest(url)
    val dropped = route(dropReq).get
    DELETEResult(url, status(dropped), wrappedResult = dropped)
  }

  def getDatabases: GETResult = {
    val get = makeGetRequest(DATABASES)
    val resp = route(get).get
    val dbRepr = contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _ => JsNull //indicates that something wrong
    }
    GETResult(API.DATABASES, status(resp), dbRepr, wrappedResult = resp)
  }

  def getDatabase(dbName: String): GETResult = {
    val get = makeGetRequest(DATABASES/dbName)
    val resp = route(get).get
    val dbRepr = contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _ => JsNull //indicates that something wrong
    }
    GETResult(DATABASES/dbName, status(resp), dbRepr, wrappedResult = resp)
  }

  def makeCollection(dbName: String, colName: String, metadata: JsObject = defaultCollectionMetadata): PUTResult = {
    Logger.info("START CREATING COLLECTION")
    val url = DATABASES/dbName/colName
    val put = makePutRequest(url, metadata)
    val created = route(put).get
    assert(created.isInstanceOf[AsyncResult])
    val st = status(created)
    Logger.info("CREATED COLLECTION")
    PUTResult(url, st,wrappedResult = created)
  }

  def getCollection(dbName: String, colName: String): GETResult = {
    Logger.info("Start GETTING Collection")
    val url = DATABASES/dbName/colName
    val get = makeGetRequest(url)
    val resp = route(get).get
    assert(resp.isInstanceOf[AsyncResult])
    GETResult(url, status(resp), contentAsJson(resp), wrappedResult = resp)
  }

  def getDownload(dbName: String, colName: String): GETResult = {
      Logger.info("Start download Collection")
      val url = DATABASES/dbName/colName/DOWNLOAD
      val get = makeGetRequest(url)
      val resp : Result = route(get).get
      GETResult(url, status(resp), contentAsJsonStream(resp), wrappedResult = resp)
  }

  def postMediaObject(dbName: String, colName: String, mediaObject: JsObject) : POSTResult = {
    val url = DATABASES/dbName/colName/MEDIA
    val post = makePostRequest(url, mediaObject)
    val posted = route(post).get
    val resp = contentAsJson(posted) match {
      case obj : JsObject => obj
      case _ => JsNull
    }
    POSTResult(url, status(posted), requestBody = mediaObject, responseBody = resp, wrappedResult = posted)
  }

  def getMediaObject(url: String): GETResult = {
      val get = makeGetRequest(url)
      val resp = route(get).get
      val mediaRepr = contentAsJson(resp) match {
        case obj: JsObject => obj
        case _ => JsNull //indicates that something wrong
      }
      GETResult(url, status(resp), mediaRepr, wrappedResult = resp)
  }


  def deleteMediaObject(url: String): DELETEResult = {
     val delReq = makeDeleteRequest(url)
     val deleted = route(delReq).get
     DELETEResult(url, status(deleted), wrappedResult = deleted)
  }

  //TODO simplify this code by introducing GETResult.apply(url)
  def getViews(dbName: String, colName: String): GETResult = {
    val url = DATABASES/dbName/colName/VIEWS
    val get = makeGetRequest(url)
    val resp = route(get).get
    GETResult(url, status(resp), contentAsJson(resp), wrappedResult = resp)
  }
  
  def onDatabase[T](db: String, app: FakeApplication = FakeApplication())(block: => T) {
    running(app){
      try {
        makeDatabase(db)
        block
      }finally {
        dropDatabase(db)
      }
    }
  }

  /**
   * Ensures that the database and collection exist, and then execute the block
   *
   * @param db the specified database that must exist
   * @param col the specified collection that must exist
   * @param app the FakeApplication to use
   * @param block the code to execute
   * @tparam T type of the block
   */
  def onCollection[T](db: String, col: String, app: FakeApplication  = FakeApplication())(block: => T) {
    onDatabase(db,app){
      makeCollection(db,col)
      block
    }
  }

  def loadData[B](db: String, col: String, data: Array[Byte]) = {
    Logger.info("START LOADING TEST DATA")
    val url = DATABASES/db/col/TX/INSERT
    val post = makePostRequest(url, data)
    val resp = route(post).get
    val st = status(resp)
    Logger.info("LOADED TEST DATA")
    if (st != OK) throw new IllegalStateException("Failure to load test data.")
    POSTResult(url, st, contentAsJson(resp), wrappedResult = resp)
  }

  def withData[B, T](db: String, col: String, data: Array[Byte], app: FakeApplication = FakeApplication())(block: => T) {
    onCollection(db,col) {
      loadData(db,col, data)
      block
    }
  }
  

}

object UtilityMethods {

  import play.api.libs.concurrent._

  implicit val defaultExtent = new Envelope(0,0,90,90, CrsId.valueOf(4326))
  val defaultIndexLevel = 4

  val defaultCollectionMetadata = Json.obj(
    "extent" -> Json.obj("crs" -> defaultExtent.getCrsId.getCode, "envelope" ->
      Json.arr(defaultExtent.getMinX,defaultExtent.getMinY,defaultExtent.getMaxX,defaultExtent.getMaxY)),
    "index-level" -> defaultIndexLevel
  )

  def makeGetRequest(url: String) = FakeRequest(GET, url).withHeaders("Accept" -> "application/json")


  def makePutRequest(url: String, body: JsValue) = FakeRequest(PUT, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body)

  def makePostRequest(url: String, body: JsValue) = FakeRequest(POST, url)
     .withHeaders("Accept" -> "application/json")
     .withJsonBody(body)

  def makePostRequest[B](url: String, body: Array[Byte] ) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withRawBody(body)

  def makeDeleteRequest(url: String) = FakeRequest(DELETE, url)

  def contentAsJson(result: Result): JsValue = {
    val responseText = contentAsString(result)
    try {
      Json.parse(responseText)
    } catch {
      case ex : Throwable => {
        if(! responseText.isEmpty) Logger.warn("Error on parsing text: " + responseText)
        JsNull
      }
    }
  }

  def contentAsJsonStream(result: Result): JsArray = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val buf = ListBuffer[JsValue]()
    val consumer: Iteratee[Array[Byte], Unit] =
      Iteratee.fold[Array[Byte], ListBuffer[JsValue]]( buf) ( (buf, bytes) => {
        val s = new String(bytes,"UTF-8")
        try {  buf += Json.parse(s) } catch { case _ : Throwable => buf }
      }).map( _ => Unit)
    result match {
      case chunkedRes  : ChunkedResult[Array[Byte]]=> {
          val fUnit = chunkedRes.chunks(consumer).asInstanceOf[ Future[Iteratee[Any, Unit]]]
          Await.result(fUnit, 10 second)
          JsArray(buf.toSeq)
      }
      case AsyncResult(p) => contentAsJsonStream(p.await.get)
      case _ => Json.arr(contentAsJson(result))
    }
  }

}


object API {

  case class URLToken(token: String) {
    def /(other: URLToken): URLToken = URLToken(s"$token/${other.token}")
  }

  implicit def URLToken2String(urlToken : URLToken): String = urlToken.token

  implicit def String2URLToken(str : String): URLToken = URLToken(str)

  val DATABASES = "/api/databases"

  val TX = "tx"

  val DOWNLOAD = "download"

  val QUERY = "query"

  val INSERT = "insert"

  val REMOVE = "remove"

  val UPDATE = "update"

  val MEDIA = "media"

  val VIEWS = "views"

}