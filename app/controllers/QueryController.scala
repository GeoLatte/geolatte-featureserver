package controllers

import javax.inject.Inject

import Exceptions._
import config.AppExecutionContexts
import persistence._
import utilities.GeometryReaders._
import org.geolatte.geom.crs.CrsId
import org.geolatte.geom.{ Envelope, Geometry, Point }
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import org.supercsv.util.CsvContext
import play.api.Logger
import play.api.libs.iteratee._
import play.api.libs.json.{ JsBoolean, JsNumber, JsString, _ }
import play.api.mvc._
import querylang.{ BooleanAnd, BooleanExpr, QueryParser }
import utilities.{ EnumeratorUtility, Utils }

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.language.{ implicitConversions, reflectiveCalls }
import scala.util.{ Failure, Success, Try }

class QueryController @Inject() (val repository: Repository) extends FeatureServerController with FutureInstrumented {

  import AppExecutionContexts.streamContext
  import config.Constants._

  def parseQueryExpr(s: String): Option[BooleanExpr] = QueryParser.parse(s) match {
    case Success(expr) => Some(expr)
    case Failure(t) => throw InvalidQueryException(t.getMessage)
  }

  def parseFormat(s: String): Option[Format.Value] = s match {
    case Format(fmt) => Some(fmt)
    case _ => None
  }

  object QueryParams {

    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))

    val WITH_VIEW = QueryParam("with-view", (s: String) => Some(s))

    val LIMIT = QueryParam("limit", (s: String) => Some(s.toInt))

    val START = QueryParam("start", (s: String) => Some(s.toInt))

    val PROJECTION: QueryParam[JsArray] = QueryParam("projection", (s: String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty PROJECTION parameter")
      else Some(JsArray(s.split(',').toSeq.map(e => JsString(e)))))

    val SORT: QueryParam[JsArray] = QueryParam("sort", (s: String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty SORT parameter")
      else Some(JsArray(s.split(',').toSeq.map(e => JsString(e)))))

    val SORTDIR: QueryParam[JsArray] = QueryParam("sort-direction", (s: String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty SORT-DIRECTION parameter")
      else Some(JsArray(s.split(',').toSeq
        .map(e => {
          val dir = e.toUpperCase
          if (dir != "ASC" && dir != "DESC") JsString("ASC")
          else JsString(dir)
        }))))

    val QUERY: QueryParam[BooleanExpr] = QueryParam("query", parseQueryExpr)

    val SEP = QueryParam("sep", (s: String) => Some(s))

    val FMT: QueryParam[Format.Value] = QueryParam("fmt", parseFormat)

    val FILENAME = QueryParam("filename", (s: String) => Some(s))
  }

  case class FeatureCollectionRequest(
    bbox: Option[String],
    query: Option[BooleanExpr],
    projection: List[String],
    withView: Option[String],
    sort: List[String],
    sortDir: List[String],
    start: Int,
    limit: Option[Int],
    intersectionGeometryWkt: Option[String]
  )

  private def extractFeatureCollectionRequest(request: Request[AnyContent]) = {

    implicit val queryString: Map[String, Seq[String]] = request.queryString
    //TODO -- why is this not a query parameter
    val intersectionGeometryWkt: Option[String] = request.body.asText.flatMap {
      case x if !x.isEmpty => Some(x)
      case _ => None
    }

    val bbox = QueryParams.BBOX.extract
    val query = QueryParams.QUERY.extract
    val projection = QueryParams.PROJECTION.extract.map(_.as[List[String]]).getOrElse(List())
    val withView = QueryParams.WITH_VIEW.extract

    val sort = QueryParams.SORT.extract.map(_.as[List[String]]).getOrElse(List())
    val sortDir = QueryParams.SORTDIR.extract.map(_.as[List[String]]).getOrElse(List())

    val start = QueryParams.START.extract.getOrElse(0)
    val limit = QueryParams.LIMIT.extract

    FeatureCollectionRequest(bbox, query, projection, withView, sort, sortDir, start, limit, intersectionGeometryWkt)
  }

  def query(db: String, collection: String) =
    RepositoryAction(implicit request =>
      futureTimed("featurecollection-query") {

        implicit val queryStr = request.queryString

        implicit val format = QueryParams.FMT.extract
        implicit val filename = QueryParams.FILENAME.extract

        val ct = RequestContext(request, format, filename)

        featuresToResult(db, collection, request) {
          case (optTotal, features) => {
            val (writeable, contentType) = ResourceWriteables.selectWriteable(ct)
            val result = Ok.chunked(FeatureStream(optTotal, features).asSource(writeable)).as(contentType)
            filename match {
              case Some(fn) => result.withHeaders(headers = ("content-disposition", s"attachment; filename=$fn"))
              case _ => result
            }
          }
        }
      })

  def list(db: String, collection: String) = RepositoryAction(
    implicit request => futureTimed("featurecollection-list") {
      featuresToResult(db, collection, request) {
        case (optTotal, features) => Ok.chunked(FeaturesResource(optTotal, features).asSource)
      }
    }
  )

  def featuresToResult(db: String, collection: String, request: Request[AnyContent])(toResult: ((Option[Long], Enumerator[JsObject])) => Result): Future[Result] = {
    repository.metadata(db, collection).flatMap(md => {
      val featureCollectionRequest = extractFeatureCollectionRequest(request)
      Logger.debug(s"Query $featureCollectionRequest on $db, collection $collection")
      doQuery(db, collection, md, featureCollectionRequest).map[Result] { toResult }
    }).recover(commonExceptionHandler(db, collection))
  }

  def download(db: String, collection: String) = RepositoryAction {
    implicit request =>
      {
        Logger.info(s"Downloading $db/$collection.")
        implicit val queryStr = request.queryString

        implicit val format = QueryParams.FMT.extract
        implicit val filename = QueryParams.FILENAME.extract
        val ct = RequestContext(request, format, filename)
        (for {
          md <- repository.metadata(db, collection)
          (optTotal, features) <- repository.query(db, collection, SpatialQuery(metadata = md))
        } yield {
          val (writeable, contentType) = ResourceWriteables.selectWriteable(ct)
          val result = Ok.chunked(FeatureStream(optTotal, features).asSource(writeable)).as(contentType)
          filename match {
            case Some(fn) => result.withHeaders(headers = ("content-disposition", s"attachment; filename=$fn"))
            case _ => result
          }
        }).recover {
          commonExceptionHandler(db, collection)
        }
      }
  }

  def collectFeatures: Iteratee[JsObject, ListBuffer[JsObject]] =
    Iteratee.fold[JsObject, ListBuffer[JsObject]](ListBuffer[JsObject]())((state, feature) => {
      state.append(feature)
      state
    })

  /**
   * converts a JsObject Enumerator to an StreamingResource supporting both Json and Csv output
   *
   * Limitations: when the passed Json is not a valid GeoJson object, this will pass a stream of empty points
   *
   * @param enum enumerator
   * @param req  requestHeader
   * @return
   */
  implicit def enumJsonToResult(enum: Enumerator[JsObject])(implicit req: RequestHeader) =
    {

      val encoder = new DefaultCsvEncoder()

      val cc = new CsvContext(0, 0, 0)

      def encode(v: JsString) = "\"" + encoder.encode(v.value, cc, CsvPreference.STANDARD_PREFERENCE).replaceAll("\n", "")
        .replaceAll("\r", "") + "\""

      def expand(v: JsObject): Seq[(String, String)] =
        utilities.JsonHelper.flatten(v) sortBy {
          case (k, _) => k
        } map {
          case (k, v: JsString) => (k, encode(v))
          case (k, v: JsNumber) => (k, Json.stringify(v))
          case (k, v: JsBoolean) => (k, Json.stringify(v))
          case (k, _) => (k, "")
        }

      def project(js: JsObject)(selector: PartialFunction[(String, String), String], geomToString: Geometry => String): Seq[String] = {
        val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
        val attributes = expand(jsObj).collect(selector)
        val geom = geomToString((js \ "geometry").asOpt(GeometryReads(CrsId.UNDEFINED)).getOrElse(Point.createEmpty()))
        val idOpt = (js \ "id").asOpt[String].map(v => ("id", v)).getOrElse(("_id", "null"))
        selector(idOpt) +: geom +: attributes
      }

      implicit val queryStr = req.queryString
      val sep = QueryParams.SEP.extract.filterNot(_.isEmpty).getOrElse(",")

      val toCsvRecord = (js: JsObject) => project(js)({
        case (k, v) => v
        case _ => "None"
      }, g => s""""${g.asText}"""").mkString(sep)

      val toCsvHeader = (js: JsObject) => project(js)({
        case (k, v) => k
        case _ => "None"
      }, _ => "geometry-wkt").mkString(sep)

      val toCsv: (Int, JsObject) => String = (i, js) => Try {
        if (i != 0) toCsvRecord(js)
        else toCsvHeader(js) + "\n" + toCsvRecord(js)
      } match {
        case Success(v) => v
        case Failure(t) =>
          Utils.withError(s"Failure to encode $js in CSV. Message is: ")("")
      }

      def toJsonStream = enum

      def toCsvStream: Enumerator[String] = EnumeratorUtility.withIndex(enum).map[String](toCsv.tupled)
        .through(Enumeratee.filterNot(_.isEmpty))

    }

  private def doQuery(db: String, collection: String, smd: Metadata, request: FeatureCollectionRequest): Future[(Option[Long], Enumerator[JsObject])] = {

    val window = Bbox(request.bbox.getOrElse(""), smd.envelope.getCrsId)

    for {
      viewDef <- request.withView.map(viewId => repository.getView(db, collection, viewId)).getOrElse(Future.successful(Json.obj()))
      (viewQuery, viewProj) = viewDef.as(Formats.ViewDefExtract)
      spatialQuery = SpatialQuery(
        window,
        request.intersectionGeometryWkt,
        selectorMerge(viewQuery, request.query),
        toProjectList(viewProj, request.projection),
        toFldSortSpecList(request.sort, request.sortDir),
        smd
      )
      result <- repository.query(db, collection, spatialQuery, Some(request.start), request.limit)
    } yield result

  }

  private def selectorMerge(viewQuery: Option[String], exprOpt: Option[BooleanExpr]): Option[BooleanExpr] = {
    val res = (viewQuery, exprOpt) match {
      case (Some(str), Some(e)) => parseQueryExpr(str).map(expr => BooleanAnd(expr, e))
      case (None, s @ Some(_)) => s
      case (Some(str), _) => parseQueryExpr(str)
      case _ => None
    }
    Logger.debug(s"Merging optional selectors of view and query to: $res")
    res
  }

  private def toProjectList(viewProj: Option[JsArray], qProj: List[String]): List[String] = {
    val vp1 = viewProj.flatMap(jsa => jsa.asOpt[List[String]]).getOrElse(List())
    val result = vp1 ++ qProj
    Logger.debug(s"Merging optional projectors of view and query to: $result")
    result
  }

  private def toFldSortSpecList(sortFldList: List[String], sortDirList: List[String]): List[FldSortSpec] = {
    val fldDirStrings = if (sortFldList.length < sortDirList.length) sortDirList.take(sortFldList.length) else sortDirList
    //we are guaranteed that fldDirs is in length shorter or equal to sortFldList
    val fldDirs = fldDirStrings.map {
      case Direction(dir) => dir
      case _ => ASC
    }
    sortFldList
      .zipAll(fldDirs, "", ASC)
      .map { case (fld, dir) => FldSortSpec(fld, dir) }
  }

  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) =>
          try {
            val env = new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs)
            if (!env.isEmpty) Some(env)
            else None
          } catch {
            case _: Throwable => None
          }
        case _ => None
      }
    }
  }

}