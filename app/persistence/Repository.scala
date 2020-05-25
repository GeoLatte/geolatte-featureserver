package persistence

import java.sql.Timestamp

import akka.stream.scaladsl.Source
import controllers.{ IndexDef, IndexDefW }
import org.geolatte.geom.Envelope
import persistence.GeoJsonFormats._
import persistence.querylang.{ BooleanExpr, ProjectionList, SimpleProjection }
import play.api.libs.functional.syntax._
import play.api.libs.json.JsonValidationError
import play.api.libs.json._

import scala.concurrent.Future

case class Metadata(
  name:           String,
  envelope:       Envelope,
  level:          Int,
  idType:         String,
  count:          Long     = 0,
  geometryColumn: String   = "GEOMETRY",
  pkey:           String   = "id",
  jsonTable:      Boolean  = true // is table defined by persistence Server? or registered
)

object MetadataIdentifiers {
  val MetadataCollectionPrefix = "geolatte_nosql_"
  val MetadataCollection = "geolatte_nosql_collections"
  val ViewCollection = "geolatte_nosql_views"
  val ExtentField = "extent"
  val IndexLevelField = "index-level"
  val CollectionField = "collection"
  val IdTypeField = "id-type"
  val CountField = "count"
  val GeometryColumnField = "geometry-column"
  val PkeyField = "primary-key"
  val IsJsonField = "is-json"
}

object Metadata {

  import MetadataIdentifiers._

  //added so that MetadataReads compiles
  def fromReads(name: String, envelope: Envelope, level: Int, idType: String): Metadata =
    this(name, envelope, level, idType)

  implicit val MetadataReads = (
    (__ \ CollectionField).read[String] and
    (__ \ ExtentField).read[Envelope](EnvelopeFormats) and
    (__ \ IndexLevelField).read[Int] and
    (__ \ IdTypeField).read[String](Reads.filter[String](JsonValidationError("Requires 'text' or 'decimal"))(tpe => tpe == "text" || tpe == "decimal"))
  )(Metadata.fromReads _)
}

case class PostQuery(wkt: Option[String] = None, query: Option[String] = None)

sealed trait Direction

object ASC extends Direction {
  override def toString = "ASC"
}

object DESC extends Direction {
  override def toString = "DESC"
}

object Direction {
  def unapply(s: String): Option[Direction] =
    if (s.toUpperCase == "DESC") Some(DESC)
    else if (s.toUpperCase == "ASC") Some(ASC)
    else None
}

case class FldSortSpec(fld: String, direction: Direction)

case class SpatialQuery(
  windowOpt:                  Option[Envelope]       = None,
  intersectionGeometryWktOpt: Option[String]         = None,
  queryOpt:                   Option[BooleanExpr]    = None,
  projection:                 Option[ProjectionList] = None,
  sort:                       List[FldSortSpec]      = List(),
  metadata:                   Metadata,
  withCount:                  Boolean                = false,
  explode:                    Boolean                = false
)

trait FeatureWriter {

  def insert(features: Seq[JsObject]): Future[Int]
  def upsert(features: Seq[JsObject]): Future[Int]

}

case class TableStats(
  schemaName:            String,
  relName:               String,
  numberLiveTup:         Long,
  numberDeadTup:         Long,
  numberModSinceAnalyse: Long,
  lastVacuum:            Option[Timestamp],
  lastAutoVacuum:        Option[Timestamp],
  lastAnalyze:           Option[Timestamp],
  lastAutoAnalyze:       Option[Timestamp],
  vacuumCount:           Long,
  autovacuumCount:       Long,
  analyzeCount:          Long,
  autoanalyzeCount:      Long
)

case class ActivityStats(
  pid:           Long,
  name:          String,
  xactStart:     Option[Timestamp],
  queryStart:    Option[Timestamp],
  waitEventType: Option[String],
  waitEvent:     Option[String],
  state:         Option[String],
  query:         Option[String]
)

/**
 * Exposes "Health" information on the Repository
 */
trait RepoHealth {

  def getActivityStats: Future[Vector[ActivityStats]]

  def getTableStats: Future[Vector[TableStats]]

}

/**
 * *
 * The feature repository
 * Created by Karel Maesen, Geovise BVBA on 08/12/14.
 */
trait Repository {

  /**
   * QueryResults are pair of and Optional total number of objects that are returned by the query (disregarding limit
   * and start query-params), and an enumerator for the results (respecting limit and start params.
   */
  type CountedQueryResult = (Option[Long], Source[JsObject, _])

  def listDatabases: Future[List[String]]

  def createDb(dbname: String): Future[Boolean]

  def dropDb(dbname: String): Future[Boolean]

  def count(database: String, collection: String): Future[Long]

  def metadata(database: String, collection: String, withCount: Boolean = false): Future[Metadata]

  def listCollections(dbname: String): Future[List[String]]

  def existsCollection(dbName: String, colName: String): Future[Boolean]

  def createCollection(dbName: String, colName: String, spatialSpec: Metadata): Future[Boolean]

  def registerCollection(db: String, collection: String, metadata: Metadata): Future[Boolean]

  def deleteCollection(dbName: String, colName: String): Future[Boolean]

  def query(database: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
            limit: Option[Int] = None): Future[CountedQueryResult]

  def distinct(database: String, collection: String, spatialQuery: SpatialQuery, projection: SimpleProjection): Future[List[String]]

  def delete(database: String, collection: String, query: BooleanExpr): Future[Boolean]

  def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int]

  def writer(database: String, collection: String): FeatureWriter

  /**
   * Saves a view for the specified database and collection.
   *
   * @param database the database for the view
   * @param collection the collection for the view
   * @param viewDef the view definition
   * @return eventually true if this save resulted in the update of an existing view, false otherwise
   */
  def saveView(database: String, collection: String, viewDef: JsObject): Future[Boolean]

  def getViews(database: String, collection: String): Future[List[JsObject]]

  def getView(database: String, collection: String, id: String): Future[JsObject]

  def dropView(database: String, collection: String, id: String): Future[Boolean]

  def createIndex(dbName: String, colName: String, indexDef: IndexDef): Future[Boolean]

  def getIndices(database: String, collection: String): Future[List[String]]

  def getIndex(database: String, collection: String, index: String): Future[IndexDefW]

  def dropIndex(database: String, collection: String, index: String): Future[Boolean]

}
