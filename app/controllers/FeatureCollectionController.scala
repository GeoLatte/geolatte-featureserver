package controllers

import org.supercsv.util.CsvContext
import querylang.{BooleanAnd, QueryParser, BooleanExpr}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable
import scala.language.reflectiveCalls
import scala.language.implicitConversions

import org.geolatte.geom.{Point, Geometry, Envelope}
import org.geolatte.geom.crs.CrsId
import play.api.mvc._
import play.api.Logger
import play.api.libs.iteratee._
import config.AppExecutionContexts
import play.api.libs.json._
import nosql.json.GeometryReaders._
import org.supercsv.encoder.DefaultCsvEncoder
import org.supercsv.prefs.CsvPreference
import scala.concurrent.Future

import play.api.libs.json.JsString
import play.api.libs.json.JsBoolean

import play.api.libs.json.JsNumber
import utilities.{EnumeratorUtility, QueryParam}
import nosql.InvalidQueryException
import play.api.libs.json.JsObject
import nosql.{Metadata, SpatialQuery, FutureInstrumented}

import scala.util.{Try, Failure, Success}


object FeatureCollectionController extends AbstractNoSqlController with FutureInstrumented {

  import AppExecutionContexts.streamContext

  import config.ConfigurationValues._
  val COLLECTION_LIMIT = MaxReturnItems


  def parseQueryExpr(s: String) : Option[BooleanExpr]= QueryParser.parse(s) match {
    case Success(expr) => Some(expr)
    case Failure(t) => throw InvalidQueryException(t.getMessage)
  }

  def parseFormat(s: String) : Option[Format.Value] = s match {
    case Format(fmt) => Some(fmt)
    case _ => None
  }

  object QueryParams {

    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))

    val WITH_VIEW = QueryParam("with-view", (s: String) => Some(s))

    val LIMIT = QueryParam("limit", (s:String) => Some(s.toInt))

    val START = QueryParam("start", (s:String) => Some(s.toInt))

    val PROJECTION : QueryParam[JsArray] = QueryParam("projection", (s:String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty PROJECTION parameter")
      else Some(JsArray( s.split(',').toSeq.map(e => JsString(e)) ))
    )

    val SORT: QueryParam[JsArray] = QueryParam("sort", (s: String) =>
      if (s.isEmpty) throw InvalidQueryException("Empty SORT parameter")
      else Some(JsArray( s.split(',').toSeq.map(e => JsString(e)) ))
    )

    val QUERY : QueryParam[BooleanExpr] = QueryParam("query", parseQueryExpr )

    val SEP = QueryParam("sep", (s: String) => Some(s))

    val FMT : QueryParam[Format.Value] = QueryParam("fmt", parseFormat )

  }



  def query(db: String, collection: String) =
    repositoryAction ( implicit request =>
      futureTimed("featurecollection-query"){
        implicit val queryStr = request.queryString

        val start : Option[Int] = QueryParams.START.extract
        val limit : Option[Int]= QueryParams.LIMIT.extract
        implicit val format = QueryParams.FMT.extract

        Logger.info(s"Query string $queryStr on $db, collection $collection")
          repository.metadata(db, collection).flatMap(md =>
            doQuery(db, collection, md, start, limit).map{
              case (_, x) => enumJsonToResult(x)
            }.map{
              x => toSimpleResult(x)
            }
          ).recover {
            case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
          }.recover (commonExceptionHandler(db, collection))
      }
    )


  def download(db: String, collection: String) = repositoryAction {
      implicit request => {
        Logger.info(s"Downloading $db/$collection.")
        implicit val format = QueryParams.FMT.extract(request.queryString)
        repository.query(db, collection, SpatialQuery()).map {
          case (_, x) => enumJsonToResult(x)
        }.map{
          x => toSimpleResult(x)
        }.recover {
          commonExceptionHandler(db, collection)
        }
      }
    }


  def collectFeatures : Iteratee[JsObject, ListBuffer[JsObject]] =
    Iteratee.fold[JsObject, ListBuffer[JsObject]]( ListBuffer[JsObject]() ) ( (state, feature) =>
      {state.append(feature); state}
    )

  def list(db: String, collection: String) = repositoryAction(
    implicit request => futureTimed("featurecollection-list"){
      implicit val queryStr = request.queryString

      val limit = Math.min( QueryParams.LIMIT.extract.getOrElse(COLLECTION_LIMIT),COLLECTION_LIMIT)
      val start = QueryParams.START.extract.getOrElse(0)

      Logger.info(s"Query string $queryStr on $db, collection $collection")

      repository.metadata(db, collection).flatMap(md =>
        doQuery(db, collection, md, Some(start), Some(limit)).flatMap {
          case ( optTotal , enum) => (enum |>>> collectFeatures) map ( l => (optTotal, l.toList))
        }.map[SimpleResult]{
          case (optTotal, features) => toSimpleResult(FeaturesResource(optTotal, features))
        }
      ).recover {
        case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
      }.recover (commonExceptionHandler(db, collection))

    }
  )

  /**
   * converts a JsObject Enumerator to an RenderableStreamingResource supporting both Json and Csv output
   *
   * Limitations: when the passed Json is not a valid GeoJson object, this will pass a stream of empty points
   * @param enum
   * @param req
   * @return
   */
  implicit def enumJsonToResult(enum: Enumerator[JsObject])(implicit req: RequestHeader) =
    new JsonStreamable with CsvStreamable {

      val encoder = new DefaultCsvEncoder()

      val cc = new CsvContext(0,0,0)
      def encode(v: JsString) = "\"" + encoder.encode(v.value, cc, CsvPreference.STANDARD_PREFERENCE) + "\""


      def expand(v : JsObject) : Seq[(String, String)] =
        utilities.JsonHelper.flatten(v) sortBy {
          case (k,v) => k
        } map {
        case (k, v: JsString)   => (k, encode(v) )
        case (k, v: JsNumber)   => (k, Json.stringify(v) )
        case (k, v: JsBoolean)  => (k, Json.stringify(v) )
        case (k, _) => (k, "")
      }


      def project(js: JsObject)(selector: PartialFunction[(String, String), String], geomToString: Geometry => String) : Seq[String] = {
        val jsObj = (js \ "properties").asOpt[JsObject].getOrElse(JsObject(List()))
        val attributes = expand(jsObj).collect(selector)
        val geom = geomToString((js \ "geometry").asOpt(GeometryReads(CrsId.UNDEFINED)).getOrElse(Point.createEmpty()))
        val idOpt = (js \ "_id" \ "$oid").asOpt[String].map(v => ("_id", v)).getOrElse(("_id", "null"))
        selector(idOpt) +: geom +: attributes
      }

      implicit val queryStr = req.queryString
      val sep = QueryParams.SEP.extract.filterNot( _.isEmpty ).getOrElse(",")

      val toCsvRecord = (js: JsObject) => project(js)({
        case (k, v) => v
        case _ => "None"
      }, g => g.asText).mkString(sep)

      val toCsvHeader = (js: JsObject) => project(js)({
        case (k, v) => k
        case _ => "None"
      }, _ => "geometry-wkt").mkString(sep)

      val toCsv : (Int, JsObject) => String = (i,js) => Try {
          if (i != 0) toCsvRecord(js)
          else toCsvHeader(js)  + "\n" + toCsvRecord(js)
      } match {
        case Success(v) => v
        case Failure(t) => {
          Logger.error(s"Failure to encode $js in CSV. Message is: ")
          ""
        }
      }

      def toJsonStream = enum

      override def toCsvStream: Enumerator[String] = EnumeratorUtility.withIndex(enum).map[String]( toCsv.tupled).through(Enumeratee.filterNot(_.isEmpty))

    }

  private def doQuery(db: String, collection: String, smd: Metadata, start: Option[Int] = None, limit: Option[Int] = None)
                            (implicit queryStr: Map[String, Seq[String]]) : Future[(Option[Long], Enumerator[JsObject])] =
    queryString2SpatialQuery(db,collection,smd).flatMap( q =>  repository.query(db, collection, q, start, limit) )


  implicit def queryString2SpatialQuery(db: String, collection: String, smd: Metadata)
                                       (implicit queryStr: Map[String, Seq[String]]) : Future[SpatialQuery] = {
    val windowOpt = Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId)
    val projectionOpt = QueryParams.PROJECTION.extract
    val queryParamOpt = QueryParams.QUERY.extract
    val sortParamOpt  = QueryParams.SORT.extract
    val viewDef = QueryParams.WITH_VIEW.extract.map(vd => repository.getView(db, collection, vd))
      .getOrElse(Future {
      Json.obj()
    })

    viewDef.map(vd => vd.as(Formats.ViewDefExtract))
      .map {
      case (queryOpt, projOpt) =>
        SpatialQuery(
          windowOpt,
          selectorMerge(queryOpt, queryParamOpt),
          projectionMerge(projOpt, projectionOpt),
          sortParamOpt
        )
    }

  }

  private def selectorMerge(viewQuery: Option[String], exprOpt : Option[BooleanExpr]) : Option[BooleanExpr] = {

    val res = (viewQuery, exprOpt) match {
      case (Some(str), Some(e)) => parseQueryExpr(str).map(expr => BooleanAnd(expr, e))
      case (None, s @ Some(_)) => s
      case (Some(str), _) => parseQueryExpr(str)
      case _ => None
    }
    Logger.debug(s"Merging optional selectors of view and query to: $res")
    res
  }

  private def projectionMerge(viewProj: Option[JsArray], qProj: Option[JsArray]): Option[JsArray] = {
    val vp1 = viewProj.getOrElse(Json.arr())
    val vp2 = qProj.getOrElse(Json.arr())
    val combined = vp1 ++ vp2
    val result = if (combined.value.isEmpty) None else Some(combined)
    Logger.debug(s"Merging optional projectors of view and query to: $result")
    result
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


}