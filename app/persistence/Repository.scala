package persistence

import akka.stream.scaladsl.Source
import controllers.IndexDef
import utilities.GeometryReaders._
import org.geolatte.geom.Envelope
import org.geolatte.geom.curve.MortonCode
import play.api.data.validation.ValidationError
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.libs.functional.syntax._
import querylang.BooleanExpr

import scala.concurrent.Future
import scala.util.{ Failure, Success }

case class Metadata(
  name: String,
  envelope: Envelope,
  level: Int,
  idType: String,
  count: Long = 0,
  geometryColumn: String = "GEOMETRY",
  pkey: String = "id",
  jsonTable: Boolean = true // is table defined by persistence Server? or registered
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
    (__ \ IdTypeField).read[String](Reads.filter[String](ValidationError("Requires 'text' or 'decimal"))(tpe => tpe == "text" || tpe == "decimal"))
  )(Metadata.fromReads _)
}

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
  windowOpt: Option[Envelope] = None,
  intersectionGeometryWktOpt: Option[String] = None,
  queryOpt: Option[BooleanExpr] = None,
  projection: List[String] = List(),
  sort: List[FldSortSpec] = List(),
  metadata: Metadata
)

trait FeatureWriter {

  def add(features: Seq[JsObject]): Future[Int]

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

  def metadata(database: String, collection: String): Future[Metadata]

  def listCollections(dbname: String): Future[List[String]]

  def existsCollection(dbName: String, colName: String): Future[Boolean]

  def createCollection(dbName: String, colName: String, spatialSpec: Metadata): Future[Boolean]

  def registerCollection(db: String, collection: String, metadata: Metadata): Future[Boolean]

  def deleteCollection(dbName: String, colName: String): Future[Boolean]

  def query(database: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
    limit: Option[Int] = None): Future[CountedQueryResult]

  def insert(database: String, collection: String, json: JsObject): Future[Int]

  def upsert(database: String, collection: String, json: JsObject): Future[Int]

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

  def getIndex(database: String, collection: String, index: String): Future[IndexDef]

  def dropIndex(database: String, collection: String, index: String): Future[Boolean]

}
