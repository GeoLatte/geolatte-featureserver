package controllers

import javax.inject.Inject
import Exceptions._
import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.scaladsl.Source
import config.AppExecutionContexts
import metrics.{ Instrumentation, Operation }
import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import persistence._
import persistence.querylang._
import play.api.Logger
import play.api.libs.json.{ JsString, _ }
import play.api.mvc._

import scala.concurrent.Future
import scala.language.{ implicitConversions, reflectiveCalls }
import scala.util.{ Failure, Success }

class QueryController @Inject() (val repository: Repository, val instrumentation: Instrumentation, actorSystem: ActorSystem) extends FeatureServerController {

  import AppExecutionContexts.streamContext
  import config.Constants._

  import akka.event.LoggingAdapter

  implicit val akkLogging: LoggingAdapter = Logging.getLogger(actorSystem.eventStream, play.api.Logger.logger.getName)

  def parseQueryExpr(s: String): Option[BooleanExpr] = QueryParser.parse(s) match {
    case Success(expr) => Some(expr)
    case Failure(t) => throw InvalidQueryException(s"Failure in parsing QUERY parameter: ${t.getMessage}")
  }

  def parseProjectionExpr(s: String): Option[ProjectionList] = if (s.isEmpty) None
  else ProjectionParser.parse(s) match {
    case Success(ppl) if ppl.isEmpty => None
    case Success(ppl) => Some(ppl)
    case Failure(t) => throw InvalidQueryException(s"Failure in parsing PROJECTION parameter: ${t.getMessage}")
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

    val EXPLODE = QueryParam("explode", (s: String) => Some(true))

    val START = QueryParam("start", (s: String) => Some(s.toInt))

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

    val PROJECTION: QueryParam[ProjectionList] = QueryParam("projection", parseProjectionExpr)

    val SEP = QueryParam("sep", (s: String) => Some(s))

    val FMT: QueryParam[Format.Value] = QueryParam("fmt", parseFormat)

    val FILENAME = QueryParam("filename", (s: String) => Some(s))
  }

  case class FeatureCollectionRequest(
    bbox: Option[String],
    query: Option[BooleanExpr],
    projection: Option[ProjectionList],
    withView: Option[String],
    sort: List[String],
    sortDir: List[String],
    start: Int,
    limit: Option[Int],
    intersectionGeometryWkt: Option[String],
    withCount: Boolean = false,
    explode: Boolean = false
  )

  case class DistinctRequest(
    bbox: Option[String],
    query: Option[BooleanExpr],
    intersectionGeometryWkt: Option[String],
    simpleProjection: SimpleProjection
  )

  def query(db: String, collection: String) = RepositoryAction { implicit request =>
    {
      implicit val format = QueryParams.FMT.value
      implicit val filename = QueryParams.FILENAME.value
      for {
        fcr <- extractFeatureCollectionRequest(request)
        res <- featuresToResult(db, collection, fcr) {
          case (optTotal, features) =>
            val (writeable, contentType) = ResourceWriteables.selectWriteable(request, QueryParams.FMT.value, QueryParams.SEP.value)

            val result = Ok.chunked(
              FeatureStream(optTotal, features)
                .asSource(writeable)
                .log("Query stream")
            ).as(contentType)

            filename match {
              case Some(fn) => result.withHeaders(headers = ("content-disposition", s"attachment; filename=$fn"))
              case _ => result
            }
        }
        _ = instrumentation.incrementOperation(Operation.QUERY_STREAM, db, collection)
      } yield res
    } recover commonExceptionHandler(db, collection)
  }

  def list(db: String, collection: String) = RepositoryAction(
    implicit request => {
      for {
        fcr <- extractFeatureCollectionRequest(request).map(_.copy(withCount = true))
        res <- featuresToResult(db, collection, fcr) {
          case (optTotal, features) => Ok.chunked(FeaturesResource(optTotal, features).asSource)
        }
        _ = instrumentation.incrementOperation(Operation.QUERY_COLLECTION, db, collection)
      } yield res

    } recover commonExceptionHandler(db, collection)

  )

  def distinct(db: String, collection: String) = RepositoryAction(
    implicit request => {
      for {
        fcr <- extractFeatureCollectionRequest(request)
        simpleProjection = fcr.projection
          .getOrElse(throw InvalidQueryException("Projection parameter is required")).paths.headOption.collect {
            case s: SimpleProjection => s
          }
          .getOrElse(throw InvalidQueryException("Only one simple projection supported at this time"))
        res <- distinctToResult(db, collection, DistinctRequest(fcr.bbox, fcr.query, fcr.intersectionGeometryWkt, simpleProjection)) { distinctValues =>
          Ok(Json.toJson(distinctValues))
        }
        _ = instrumentation.incrementOperation(Operation.QUERY_DISTINCT, db, collection)
      } yield res

    } recover commonExceptionHandler(db, collection)
  )

  def featuresToResult(db: String, collection: String, featureCollectionRequest: FeatureCollectionRequest)(toResult: ((Option[Long], Source[JsObject, _])) => Result): Future[Result] = {

    val fResult = for {
      md <- repository.metadata(db, collection)
      _ = Logger.debug(s"Query $featureCollectionRequest on $db, collection $collection")
      result <- doQuery(db, collection, md, featureCollectionRequest).map[Result] {
        toResult
      }
    } yield result

    fResult.recover(commonExceptionHandler(db, collection))
  }

  def distinctToResult(db: String, collection: String, distinctRequest: DistinctRequest)(toResult: List[String] => Result): Future[Result] = {

    val fResult = for {
      md <- repository.metadata(db, collection)
      _ = Logger.debug(s"Query $distinctRequest distinct on $db, collection $collection")
      result <- doDistinctQuery(db, collection, md, distinctRequest).map[Result] {
        toResult
      }
    } yield result

    fResult.recover(commonExceptionHandler(db, collection))
  }

  private def extractFeatureCollectionRequest(implicit request: Request[AnyContent]) = Future {

    implicit val queryString: Map[String, Seq[String]] = request.queryString
    //TODO -- why is this not a query parameter
    val intersectionGeometryWkt: Option[String] = request.body.asText.flatMap {
      case x if !x.isEmpty => Some(x)
      case _ => None
    }

    val bbox = QueryParams.BBOX.value
    val query = QueryParams.QUERY.value
    val projection = QueryParams.PROJECTION.value
    val withView = QueryParams.WITH_VIEW.value

    val sort = QueryParams.SORT.value.map(_.as[List[String]]).getOrElse(List())
    val sortDir = QueryParams.SORTDIR.value.map(_.as[List[String]]).getOrElse(List())

    val start = QueryParams.START.value.getOrElse(0)
    val limit = QueryParams.LIMIT.value
    val explode = QueryParams.EXPLODE.value.getOrElse(false)
    FeatureCollectionRequest(bbox, query, projection, withView, sort, sortDir, start, limit, intersectionGeometryWkt, false, explode)
  }

  private def doQuery(db: String, collection: String, smd: Metadata, request: FeatureCollectionRequest): Future[(Option[Long], Source[JsObject, _])] = {

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
        smd,
        request.withCount,
        request.explode
      )
      result <- repository.query(db, collection, spatialQuery, Some(request.start), request.limit)
      _ = instrumentation.updateSpatialQueryMetrics(db, collection, spatialQuery)
    } yield result

  }

  private def doDistinctQuery(db: String, collection: String, smd: Metadata, request: DistinctRequest): Future[List[String]] = {

    val spatialQuery = SpatialQuery(
      Bbox(request.bbox.getOrElse(""), smd.envelope.getCrsId),
      request.intersectionGeometryWkt,
      request.query,
      None,
      Nil,
      smd
    )

    for {
      result <- repository.distinct(db, collection, spatialQuery, request.simpleProjection)
      _ = instrumentation.updateSpatialQueryMetrics(db, collection, spatialQuery)
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

  private def toProjectList(viewProj: Option[JsArray], qProj: Option[ProjectionList]): Option[ProjectionList] = {
    val viewPPL = for {
      vp1 <- viewProj
      jsa <- vp1.asOpt[List[String]]
      pExp = jsa mkString ","
      ppl <- ProjectionParser.parse(pExp).toOption // we just assume that this is a valid expression, if not we ignore
    } yield ppl
    (viewPPL, qProj) match {
      case (Some(p1), Some(p2)) => Some(p1 ++ p2)
      case (_, Some(p)) => Some(p)
      case (Some(p), _) => Some(p)
      case _ => None
    }
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
