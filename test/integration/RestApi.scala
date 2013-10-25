package integration

import play.api.libs.json._
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import play.api.mvc.Result

import language.implicitConversions
import play.api.Logger

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
    val url = DATABASES/dbName
    val put = makePutRequest(url, JsNull)
    val created = route(put).get
    PUTResult(url, status(created), wrappedResult = created)
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
    val url = DATABASES/dbName/colName
    val put = makePutRequest(url, metadata)
    val created = route(put).get
    PUTResult(url, status(created),wrappedResult = created)
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

  def onCollection[T](db: String, col: String, app: FakeApplication  = FakeApplication())(block: => T) {
    onDatabase(db,app){
      makeCollection(db,col)
      block
    }
  }
  

}

object UtilityMethods {

  val defaultCollectionMetadata = Json.obj(
    "extent" -> Json.obj("crs" -> 4326, "envelope" -> Json.arr(0,0,90,90)),
    "index-level" -> 4
  )

  def makeGetRequest(url: String) = FakeRequest(GET, url).withHeaders("Accept" -> "application/json")


  def makePutRequest(url: String, body: JsValue) = FakeRequest(PUT, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body)

  def makePostRequest(url: String, body: JsValue) = FakeRequest(POST, url)
     .withHeaders("Accept" -> "application/json")
     .withJsonBody(body)


  def makeDeleteRequest(url: String) = FakeRequest(DELETE, url)

  def contentAsJson(result: Result): JsValue = {
    val responseText = contentAsString(result)
    try {
      Json.parse(responseText)
    } catch {
      case ex : Throwable => {
        Logger.warn("Error on parsing text: " + responseText)
        JsNull
      }
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

}