package nosql
import nosql.json.GeometryReaders._
import nosql.mongodb._
import org.geolatte.geom.Envelope
import org.geolatte.geom.curve.MortonCode
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.libs.functional.syntax._
import querylang.BooleanExpr

import scala.concurrent.Future
import scala.util.{Failure, Success}


case class Metadata(name: String, envelope: Envelope, level : Int, count: Long = 0)

object MetadataIdentifiers {
  val MetadataCollectionPrefix = "geolatte_nosql_"
  val MetadataCollection = "geolatte_nosql_collections"
  val ViewCollection = "geolatte_nosql_views"
  val ExtentField = "extent"
  val IndexLevelField = "index_depth"
  val CollectionField = "collection"
}

object Metadata {

  import MetadataIdentifiers._

  //added so that MetadataReads compiles
  def apply(name: String, envelope:Envelope, level: Int): Metadata = this(name, envelope,level, 0)

  implicit val MetadataReads = (
    (__ \ CollectionField).read[String] and
      (__ \ ExtentField).read[Envelope](EnvelopeFormats) and
      (__ \ IndexLevelField).read[Int]
    )(Metadata.apply _)

}

case class Media(id: String, md5: Option[String])

case class MediaReader(id: String,
                       md5: Option[String],
                       name: String,
                       len: Int,
                       contentType: Option[String],
                       data: Array[Byte])

case class SpatialQuery ( windowOpt: Option[Envelope], queryOpt: Option[BooleanExpr], projectionOpt: Option[JsArray])

object SpatialQuery {

  def apply() : SpatialQuery = apply(None, None, None)

  def apply(window: Envelope) :SpatialQuery = apply(Some(window), None, None)

  def apply(window: Envelope, query: BooleanExpr) : SpatialQuery = apply(Some(window), Some(query), None)
}


trait MortonCodeQueryOptimizer {

  //the return type of the optimizer
  type QueryDocuments

  /**
   * Optimizes the window query, given the specified MortonCode
   * @param window
   * @return
   */
  def optimize(window: Envelope, mortoncode: MortonCode): QueryDocuments
}

trait FeatureWriter {

  def add(features: Seq[JsObject]): Future[Long]

}


/**
 * Created by Karel Maesen, Geovise BVBA on 08/12/14.
 */
trait Repository {

  /**
   * QueryResults are pair of and Optional total number of objects that are returned by the query (disregarding limit
   * and start query-params), and an enumerator for the results (respecting limit and start params.
   */
  type CountedQueryResult = (Option[Long], Enumerator[JsObject])

  def listDatabases: Future[List[String]]

  def createDb(dbname: String) : Future[Boolean]

  def dropDb(dbname: String) : Future[Boolean]

  def existsDb(dbname: String): Future[Boolean]

  def count(database: String, collection: String): Future[Long]

  def metadata(database: String, collection: String): Future[Metadata]

  def listCollections(dbname: String): Future[List[String]]

  def existsCollection(dbName: String, colName: String): Future[Boolean]

  def createCollection(dbName: String, colName: String, spatialSpec: Option[Metadata]) : Future[Boolean]

  def deleteCollection(dbName: String, colName: String) : Future[Boolean]

  def query(database: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
            limit: Option[Int] = None): Future[CountedQueryResult]

  def insert(database: String, collection: String, json: JsObject) : Future[Boolean]

  def upsert(database: String, collection: String, json: JsObject) : Future[Boolean]

  def delete(database: String, collection: String, query: BooleanExpr) : Future[Boolean]

  def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject) : Future[Int]

  def writer(database: String, collection: String) : FeatureWriter

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

}
