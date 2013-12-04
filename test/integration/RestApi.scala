package integration

import play.api.libs.json._
import play.api.libs.functional.syntax._

import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}
import play.api.mvc._

import language.implicitConversions
import play.api.Logger
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import play.api.libs.iteratee.Iteratee
import play.api.libs.json.Json.JsValueWrapper
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import play.api.libs.json.JsArray
import play.api.mvc.AsyncResult
import play.api.test.FakeApplication
import play.api.mvc.ChunkedResult
import play.api.http.Writeable
import org.specs2._
import org.specs2.matcher._
import config.ConfigurationValues
import org.geolatte.geom.curve.{MortonContext, MortonCode}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/12/13
 */

case class FakeRequestResult[B,T](
  url: String,
  format: Option[Result => JsValue] = None,
  requestBody: Option[B] = None,
  mkRequest: (String, Option[B]) => FakeRequest[T])(
    implicit val w: Writeable[T]
  ) {

  val wrappedResult: Result = {
    val req = mkRequest(url, requestBody)
    route(req) match {
      case Some(res) => res
      case None => throw new RuntimeException("No route for req: " + req)
    }
  }
  val status: Int = play.api.test.Helpers.status(wrappedResult)
  val responseBody = format.map(f => f(wrappedResult))

  def applyMatcher[R]( matcher: FakeRequestResult[B,T] => MatchResult[R]) = matcher(this)

}

object FakeRequestResult {

  import UtilityMethods._

  def GET(url: String, format: Result => JsValue) =
    new FakeRequestResult[JsValue, AnyContentAsEmpty.type](url = url, format = Some(format), mkRequest = makeGetRequest)

  def PUT(url:String, body: Option[JsValue] = None) =
    new FakeRequestResult[JsValue,AnyContentAsJson](url = url, requestBody = body, mkRequest = makePutRequest)

  def DELETE(url: String) = new FakeRequestResult[JsValue, AnyContentAsEmpty.type](url = url, mkRequest = makeDeleteRequest)

  def POSTJson(url: String, body: JsValue, format: Result => JsValue) =
    new FakeRequestResult(url = url, format=Some(format), requestBody = Some(body), mkRequest = makePostRequestJson)

  def POSTRaw(url: String, body: Array[Byte], format: Result => JsValue) =
      new FakeRequestResult(url = url, format=Some(format), requestBody = Some(body), mkRequest = makePostRequestRaw)

}

//This is for pattern match
object ResultCheck {
  def unapply[B,T](fr : FakeRequestResult[B,T]) : Option[(Int, Option[JsValue])] = Some(fr.status, fr.responseBody)
}


object RestApiDriver {

  import UtilityMethods._
  import API._

  def makeDatabase(dbName: String) = {
    Logger.info("START CREATING DATABASE")
    val url = DATABASES/dbName
    FakeRequestResult.PUT(url)
  }

  def dropDatabase(dbName: String) = {
    FakeRequestResult.DELETE(DATABASES/dbName)
  }

  def getDatabases = {
    val format = (resp: Result) => contentAsJson(resp) match {
            case jArray: JsArray => jArray
            case _ => JsNull //indicates that something wrong
          }
    FakeRequestResult.GET(DATABASES, format)

  }

  def getDatabase(dbName: String) = {
    val format  = (resp: Result) => contentAsJson(resp) match {
      case jArray: JsArray => jArray
      case _ => JsNull //indicates that something wrong
    }
    FakeRequestResult.GET(DATABASES/dbName, format)
  }

  def makeCollection(dbName: String, colName: String, metadata: JsObject = defaultCollectionMetadata) = {
    Logger.info("START CREATING COLLECTION")
    val url = DATABASES/dbName/colName
    FakeRequestResult.PUT(url, Some(metadata))
  }

  def getCollection(dbName: String, colName: String) = {
    Logger.info("Start GETTING Collection")
    val url = DATABASES/dbName/colName
    FakeRequestResult.GET(url, contentAsJson)
  }

  def getDownload(dbName: String, colName: String) = {
      Logger.info("Start download Collection")
      val url = DATABASES/dbName/colName/DOWNLOAD
      FakeRequestResult.GET(url, contentAsJsonStream)
  }

  def getList(dbName: String, colName: String, queryStr: String ) = {
    Logger.info("Start /list on collection")
    val url = DATABASES/dbName/colName/FEATURECOLLECTION?queryStr
    FakeRequestResult.GET(url, contentAsJson)
  }

  def postMediaObject(dbName: String, colName: String, mediaObject: JsObject)  = {
    val url = DATABASES/dbName/colName/MEDIA

    val format = (posted: Result) => contentAsJson(posted) match {
      case obj : JsObject => obj
      case _ => JsNull
    }
    FakeRequestResult.POSTJson(url, mediaObject, format)
  }

  def getMediaObject(url: String) = {
      val format = (resp: Result) => contentAsJson(resp) match {
        case obj: JsObject => obj
        case _ => JsNull //indicates that something wrong
      }
      FakeRequestResult.GET(url, format)
  }


  def deleteMediaObject(url: String) = {
     FakeRequestResult.DELETE(url)
  }

  def getViews(dbName: String, colName: String) = {
    val url = DATABASES/dbName/colName/VIEWS
    FakeRequestResult.GET(url, contentAsJson)
  }

  @deprecated
  def onDatabase[T](db: String, app: FakeApplication = FakeApplication())(block: => T) : T = {
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
  @deprecated
  def onCollection[T](db: String, col: String, app: FakeApplication  = FakeApplication())(block: => T) : T =  {
    onDatabase(db,app){
      makeCollection(db,col)
      block
    }
  }

  def loadData[B](db: String, col: String, data: Array[Byte]) = {
    Logger.info("START LOADING TEST DATA")
    val url = DATABASES/db/col/TX/INSERT
    val res = FakeRequestResult.POSTRaw(url, data, contentAsJson)
    Logger.info("LOADED TEST DATA")
    res
  }

  def removeData(db: String, col:String) = {
    Logger.info("START REMOVING TEST DATA")
    val url = DATABASES/db/col/TX/REMOVE
    FakeRequestResult.POSTJson(url, Json.obj("query" -> Json.obj()), res => JsNull )
    Logger.info("REMOVED TEST DATA")
  }

  def withFeatures[B,T](db: String, col:String, features: JsArray)(block:  => T) = {
    val data = features.value map(j => Json.stringify(j)) mkString ConfigurationValues.jsonSeparator getBytes("UTF-8")
    loadData(db,col,data)
    try {
      block
    } finally {
      removeData(db,col)
    }
  }

}

object UtilityMethods {

  import play.api.libs.concurrent._

  val defaultIndexLevel = 4
  implicit val defaultExtent = new Envelope(0,0,90,90, CrsId.valueOf(4326))
  implicit val defaultMortonCode = new MortonCode(new MortonContext(defaultExtent, defaultIndexLevel))

  val defaultCollectionMetadata = Json.obj(
    "extent" -> Json.obj("crs" -> defaultExtent.getCrsId.getCode, "envelope" ->
      Json.arr(defaultExtent.getMinX,defaultExtent.getMinY,defaultExtent.getMaxX,defaultExtent.getMaxY)),
    "index-level" -> defaultIndexLevel
  )

  implicit def mapOfQParams2QueryStr(params: Map[String,String]) : String = params.map { case (k,v) => s"$k=$v"} mkString "&"

  def makeGetRequest(url: String, js :Option[JsValue] = None) =
    FakeRequest(GET, url).withHeaders("Accept" -> "application/json")


  def makePutRequest(url: String, body: Option[JsValue] = None) = FakeRequest(PUT, url)
    .withHeaders("Accept" -> "application/json")
    .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestJson(url: String, body: Option[JsValue]) = FakeRequest(POST, url)
     .withHeaders("Accept" -> "application/json")
     .withJsonBody(body.getOrElse(JsNull))

  def makePostRequestRaw(url: String, body: Option[Array[Byte]] ) = FakeRequest(POST, url)
    .withHeaders("Accept" -> "application/json")
    .withRawBody(body.getOrElse(Array[Byte]()))

  def makeDeleteRequest(url: String, body: Option[JsValue] = None) = FakeRequest(DELETE, url)

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

  implicit def toArrayOpt(js: Option[JsValue]) : Option[JsArray] = js match {
    case Some(ja :JsArray) => Some(ja)
    case _ => None
  }

}


object API {

  case class URLToken(token: String) {
    def /(other: URLToken): URLToken = URLToken(s"$token/${other.token}")
    def ?(other:URLToken) : URLToken = URLToken(s"$token?${other.token}")
  }

  implicit def URLToken2String(urlToken : URLToken): String = urlToken.token

  implicit def String2URLToken(str : String): URLToken = URLToken(str)

  val DATABASES = "/api/databases"

  val TX = "tx"

  val DOWNLOAD = "download"

  val QUERY = "query"

  val FEATURECOLLECTION = "featurecollection"

  val INSERT = "insert"

  val REMOVE = "remove"

  val UPDATE = "update"

  val MEDIA = "media"

  val VIEWS = "views"

}