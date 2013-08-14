package controllers

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}
import play.api.Logger
import util.{MediaTypeSpec, QueryParam}
import play.api.libs.iteratee.Enumerator
import org.geolatte.common.Feature
import org.geolatte.scala.ChainedIterator
import repositories.MongoRepository
import org.geolatte.nosql.mongodb.SpatialMetadata
import config.ConfigurationValues.{Version, Format}

object FeatureCollection extends Controller {

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))
  }

  //temporary fixed value
  val WGS_84 = CrsId.valueOf(4326)

  def query(db: String, collection: String) = Action {
    request =>
      implicit val queryStr = request.queryString
      Logger.info(s"Query string ${queryStr} on $db, collection $collection")
      doQuery(db, collection)
  }

  def download(db: String, collection: String) = Action{
    request =>
      Logger.info(s"Downloading $db/$collection.")
      if (!MongoRepository.existsCollection(db, collection)) NotFound("s${db}/${collection} not found")
      else {
        val it: Iterator[Feature] = MongoRepository.getData(db, collection)
        mkChunked(it)
      }
  }

  private def qetQueryResult(db: String, collection: String, smd: SpatialMetadata)(implicit queryStr: Map[String, Seq[String]]) = {
    Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId) match {
      case Some(window) =>
        MongoRepository.query(db, collection, window) match {
          case Left(msg) => BadRequest(msg)
          case Right(q) => mkChunked(q)
        }
      case None => BadRequest(s"BadRequest: No or invalid bbox parameter in query string.")
    }
  }

  def doQuery(db: String, collection: String)(implicit queryStr: Map[String, Seq[String]]) =
    try {
      val meta = MongoRepository.metadata(db, collection)
      val smd = for (md <- meta; s <- md.spatialMetadata) yield s
      smd match {
        case Some(smd: SpatialMetadata) => qetQueryResult(db, collection, smd)
        case None => BadRequest(s"BadRequest: Not a spatial collection.")
      }
    } catch {
      case ex: NoSuchElementException => NotFound(s"${db}.${collection} not found in metadata")
    }


  def mkChunked(it: Iterator[Feature], start: Long = System.currentTimeMillis()) = {
    try {

      val dataContent = toStream(it, start)
      Ok.stream(dataContent).as(MediaTypeSpec(Format.JSON, Version.default))
    }
    catch {
      case ex: IllegalArgumentException => BadRequest(s"Error: " + ex.getMessage())
    }
  }

  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try {
            val env = new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs)
            if (!env.isEmpty) Some(env)
            else None
          } catch {
            case _: Throwable => None
          }
        }
        case _ => None
      }
    }

  }


  def toStream(features: Iterator[Feature], startTime: Long) = {

    val jsonMapper = new JsonMapper()

    case class Counter(startTimeInMillies: Long) {
      private var num = 0

      def inc() {num += 1}

      def value = num

      def millisSinceStart = System.currentTimeMillis() - startTimeInMillies

    }

    val counter = Counter(startTime)
    import ChainedIterator._
    val START: Iterator[String] = List(s"""{"type": "FeatureCollection", "query-time": ${counter.millisSinceStart}, "features": [""").iterator
    lazy val END: Iterator[String] = List(s"""], "total":  ${counter.value}, "totalTime": ${counter.millisSinceStart}  }""").iterator

    def seperatorAddingIterator(it: Iterator[String]) = new Iterator[String] {
      def hasNext: Boolean = it.hasNext

      var sep = false

      def next(): String = if (sep) {
        sep = false; ","
      } else {
        counter.inc(); sep = true; it.next()
      }
    }

    val jsons = seperatorAddingIterator(features.map(jsonMapper.toJson(_)))

    lazy val str :Stream[Iterator[String]] = START #:: jsons #:: END #:: Stream.empty[Iterator[String]]
    val jsonStringIt = chain[String](str)

    val itStream = new java.io.InputStream {

      def advance = if (jsonStringIt.hasNext) jsonStringIt.next.iterator else Iterator.empty

      var currentJson = advance

      def hasNext: Boolean = {
        currentJson.hasNext || {
          currentJson = advance; currentJson.hasNext
        }
      }

      def read(): Int =
        if (hasNext) currentJson.next() else -1
    }

    Enumerator.fromStream(itStream)
  }

}