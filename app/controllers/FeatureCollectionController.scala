package controllers

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import org.geolatte.common.dataformats.json.jackson.JsonMapper
import play.api.mvc.{Action, Controller}
import play.api.Logger
import util.QueryParam
import play.api.libs.iteratee.Enumerator
import play.api.http.MimeTypes
import org.geolatte.common.Feature
import org.geolatte.scala.ChainedIterator
import repositories.MongoRepository

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

  def doQuery(db: String, collection: String)(implicit queryStr: Map[String, Seq[String]]) =
    try {
      val md = MongoRepository.metadata(db, collection)
      val windowOpt = Bbox(QueryParams.BBOX.extractOrElse(""), md.envelope.getCrsId)
      windowOpt match {
        case Some(window) => mkChunked(db, collection, window)
        case None => BadRequest(s"BadRequest: No or invalid bbox parameter in query string.")
      }
    } catch {
      case ex: NoSuchElementException => NotFound(s"${db}.${collection} not found in metadata")
    }


  def mkChunked(db: String, collection: String, window: Envelope) =  {
    try {
      val dataContent = toStream(MongoRepository.query(db, collection, window))
      Ok.stream(dataContent).as(MimeTypes.JSON)
    }
    catch {
      case ex : IllegalArgumentException => BadRequest(s"Error: " + ex.getMessage())
    }
  }

  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try
            Some(new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs))
          catch {
            case _: Throwable => None
          }
        }
        case _ => None
      }
    }

  }


  def toStream(features: Iterator[Feature]) = {

    val jsonMapper = new JsonMapper()

    object Counter {
      private var num = 0
      def inc : Unit = num += 1
      def value = num
    }

    import ChainedIterator._
    val START: Iterator[String] = List("{ \"items\": [").iterator

    lazy val END : Iterator[String] = List(s"], total: ${Counter.value} }").iterator



    def seperatorAddingIterator(it: Iterator[String]) = new Iterator[String] {
          def hasNext: Boolean = it.hasNext
          var sep = false
          def next(): String = if (sep) {sep = false; ","} else {Counter.inc; sep = true; it.next}
    }

    val jsons = seperatorAddingIterator( features.map( jsonMapper.toJson(_)) )


    val str  = START #:: jsons #:: END #:: Stream.empty
    val jsonStringIt = chain[String]( str )



    val itStream = new java.io.InputStream {

      def advance = if (jsonStringIt.hasNext) jsonStringIt.next.iterator else Iterator.empty

      var currentJson = advance

      def hasNext : Boolean = {
        currentJson.hasNext || {currentJson = advance; currentJson.hasNext}
      }

      def read(): Int =
        if (hasNext) currentJson.next else -1
    }

    Enumerator.fromStream(itStream)
  }

}