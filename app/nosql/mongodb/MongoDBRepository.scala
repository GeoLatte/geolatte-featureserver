package nosql.mongodb

import javax.inject._

import config.AppExecutionContexts.streamContext
import controllers.IndexDef
import nosql._
import Exceptions._
import nosql.json.GeometryReaders._
import org.geolatte.geom.Envelope
import org.reactivemongo.play.json._
import play.api.Logger
import play.api.libs.iteratee._
import play.api.libs.json._
import play.modules.reactivemongo.json.BSONFormats
import play.modules.reactivemongo.json.collection.JSONCollection
import querylang.BooleanExpr
import reactivemongo.api.{FailoverStrategy, _}
import reactivemongo.api.collections.bson.BSONCollectionProducer
import reactivemongo.api.commands._
import reactivemongo.api.gridfs.{DefaultFileToSave, GridFS}
import reactivemongo.api.indexes.Index
import reactivemongo.api.indexes.IndexType.Ascending
import reactivemongo.bson._
import reactivemongo.core.commands.{GetLastError, _}
import utilities.JsonHelper

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object MongoTypes {
  type SpatialCollection = MongoSpatialCollection
  type MediaStore = GridFS[BSONSerializationPack.type]

  /**
    * combines the JSONCollection, Metadata and transformer.
    *
    * The transformer is a Reads[JsObject] that transform an input Feature to a Feature for persistence
    * in the JSONCollection
    */
  type CollectionInfo = (JSONCollection, Metadata, Reads[JsObject])

}

@Singleton
class MongoDBRepository extends nosql.Repository with FutureInstrumented {


  val awaitJournalCommit = GetLastError(j = true, w = Some(BSONInteger(1)))

  import MongoTypes._
  import MetadataIdentifiers._
  import config.ConfigurationValues._

  val driver = new MongoDriver
  val dbconStr = MongoConnnectionString

  val connection = driver.connection(dbconStr)

  private val systemDatabase = MongoSystemDB

  private val createdDBColl = {
    connection(systemDatabase).collection[JSONCollection](DEFAULT_CREATED_DBS_COLLECTION)
  }

  def listDatabases: Future[List[String]] = {
    val futureJsons = createdDBColl.find(Json.obj()).cursor[JsObject](ReadPreference.primary).collect[List]()
    futureJsons.map(_.map(
      json => (json \ DEFAULT_CREATED_DB_PROP).as[String]))

  }

  def getMetadata(dbName: String) = connection(dbName).collection[JSONCollection](MetadataCollection)

  private def isMetadata(name: String): Boolean = (name startsWith MetadataCollectionPrefix) || (name startsWith "system.")

  private def isSpecialCollection(name: String): Boolean = isMetadata(name) ||
    name.endsWith(".files") ||
    name.endsWith(".chunks") ||
    name.endsWith(".views")

  private def expr2JsObject(expr: BooleanExpr) : JsObject = {
    MongoDBQueryRenderer.render(expr) match {
      case js if js.isInstanceOf[JsObject] => js.asInstanceOf[JsObject]
      case _ => throw new IllegalStateException("Boolean expression can't be rendered to a JSON-object")
    }
  }

  //this methods re-creates the previously supported 'save' semantics
  private def save(collection: JSONCollection, json : JsObject) : Future[WriteResult] = {
    import reactivemongo.bson.BSONObjectID
    (json \ "_id").toOption match {
      case None => collection.insert(json + ("_id" ->
        BSONFormats.BSONObjectIDFormat.writes(BSONObjectID.generate)), WriteConcern.Default)

      case Some(id) =>
        collection.update(Json.obj("_id" -> id), json, WriteConcern.Default, upsert = true)
    }
  }



  def createDb(dbname: String) = {

    //Logs the database creation in the "created databases" collection in the systemDatabase
    def registerDbCreation(dbname: String) = save(createdDBColl, Json.obj(DEFAULT_CREATED_DB_PROP -> dbname)).andThen {
      case Success(le) => Logger.debug(s"Registering $dbname in created databases collections succeeded.")
      case Failure(le) => Logger.warn(s"Registering $dbname in created databases collections succeeded.")
    }

    existsDb(dbname).flatMap {
      case false =>
        val db = connection(dbname)
        val newColl = db.collection(name=MetadataCollection)
        newColl.create(autoIndexId = true)
          .flatMap(_ => registerDbCreation(dbname))
          .map(le => true)
          .recover {
          case ex: BSONCommandError => throw new DatabaseCreationException(s"Failure to create $dbname: can't create metadatacollection")
          case ex: Throwable => throw new DatabaseCreationException(s"Unknown exception of type: ${ex.getClass.getCanonicalName} having message: ${ex.getMessage}")
        }

      case _ => throw new DatabaseAlreadyExistsException(s"Database $dbname already exists.")
    }

  }

  def dropDb(dbname: String) = {

    def removeLog(dbname: String) = createdDBColl.remove(Json.obj(DEFAULT_CREATED_DB_PROP -> dbname)).andThen {
      case Success(le) => Logger.info(s"Database $dbname dropped")
      case Failure(t) => Logger.warn(s"Database $dbname dropped, but could not register drop in $systemDatabase/$DEFAULT_CREATED_DBS_COLLECTION. \nReason: ${t.getMessage}")
    }

    existsDb(dbname).flatMap {
      case true => connection(dbname).drop().flatMap(_ => removeLog(dbname)).map(le => true).recover {
        case ex: Throwable =>
          Logger.error(s"Problem deleting database $dbname", ex)
          throw new DatabaseDeleteException(s"Unknown exception of type: ${ex.getClass.getCanonicalName} having message: ${ex.getMessage}")
      }
      case _ => throw new DatabaseNotFoundException(s"No database $dbname")
    }
  }

  def existsDb(dbname: String): Future[Boolean] = listDatabases.map(l => l.contains( dbname ))


  def listCollections(dbname: String): Future[List[String]] =
    existsDb(dbname).flatMap {
      case true => connection(dbname).collectionNames.map(_.filterNot(isSpecialCollection))
      case _ => throw new DatabaseNotFoundException(s"database $dbname doesn't exist")
    }


  def count(database: String, collectionName: String): Future[Long] =
    for { i <- connection(database).collection(collectionName).count() }
        yield i.toLong

  def metadata(database: String, collection: String): Future[Metadata] = {
    import Metadata._
    import MetadataIdentifiers._

    def readMetadata() = {
      val metaCollection = connection(database).collection[JSONCollection](MetadataCollection)
      val metadataCursor = metaCollection.find(Json.obj(CollectionField -> collection)).cursor[JsObject](ReadPreference.primary)
      metadataCursor.headOption.map {
        case Some(doc) => doc.asOpt[Metadata]
        case _ => None
      }
    }

    def mkMetadata() = for {
      mdOpt <- readMetadata()
      cnt <- count(database, collection)
    } yield mdOpt.map(md => md.copy(count = cnt)).getOrElse(Metadata(collection, Envelope.EMPTY, 0, "string", cnt))

    existsDb(database).flatMap(existsDb =>
      if (existsDb) existsCollection(database, collection)
      else throw DatabaseNotFoundException()
    ).flatMap(existsCollection =>
      if (existsCollection) mkMetadata()
      else throw CollectionNotFoundException()
      )
  }

  //we check also the "hidden" names (e.g. metadata collection) so that puts will fail
  def existsCollection(dbName: String, colName: String): Future[Boolean] = for {
    names <- connection(dbName).collectionNames
    found = names.contains( colName )
  } yield found


  override def createCollection(dbName: String, colName: String, md: Metadata) = {

    def doCreateCollection() = connection(dbName).collection[JSONCollection](colName).create().andThen {
      case Success(b) => Logger.info(s"collection $colName created: $b")
      case Failure(t) => Logger.error(s"Attempt to create collection $colName threw exception: ${t.getMessage}")
    }

    def saveMetadata = connection(dbName).collection[JSONCollection](MetadataCollection).insert(Json.obj(
        ExtentField -> md.envelope,
        IndexLevelField -> md.level,
        IdTypeField -> md.idType,
        CollectionField -> colName),
        commands.WriteConcern.Journaled
      ).andThen {
        case Success(le) => Logger.info(s"Writing metadata for $colName has result: ${le.ok}")
        case Failure(t) => Logger.error(s"Writing metadata for $colName threw exception: ${t.getMessage}")
      }.map(_ => true)


    def ensureIndexes = {
      val idxManager = connection(dbName).collection[JSONCollection](colName).indexesManager
      idxManager.ensure(new Index(Seq((SpecialMongoProperties.MC, Ascending))))
      idxManager.ensure(new Index(key = Seq((SpecialMongoProperties.ID, Ascending)), unique = true))
    }

    existsDb(dbName).flatMap(dbExists =>
      if (dbExists) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap(collectionExists =>
      if (!collectionExists) doCreateCollection()
      else throw new CollectionAlreadyExistsException()
      )
      .flatMap(_ => saveMetadata)
      .flatMap(_ => ensureIndexes)
  }

  def deleteCollection(dbName: String, colName: String) = {

    def doDeleteCollection() = connection(dbName).collection[JSONCollection](colName, FailoverStrategy()).drop().andThen {
      case Success(b) => Logger.info(s"Deleting $dbName/$colName: $b")
      case Failure(t) => Logger.warn(s"Delete of $dbName/$colName failed: ${t.getMessage}")
    }

    def removeMetadata() = connection(dbName).collection[JSONCollection](MetadataCollection, FailoverStrategy()).remove(Json.obj(CollectionField -> colName))
      .map(last => true)
      .andThen {
      case Success(b) => Logger.info(s"Removing metadata for $dbName/$colName")
      case Failure(t) => Logger.warn(s"Removing of $dbName/$colName failed: ${t.getMessage}")
    }

    //TODO -- remove also fs.<colName> gridFs and views collections

    existsDb(dbName).flatMap(dbExists =>
      if (dbExists) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap(collectionExists =>
      if (collectionExists) doDeleteCollection()
      else throw new CollectionNotFoundException()
      ).flatMap(_ => removeMetadata())

  }


  def getCollection(dbName: String, colName: String): Future[(JSONCollection, Metadata)] =
    existsDb(dbName).flatMap(dbExists =>
      if (dbExists) existsCollection(dbName, colName)
      else throw new DatabaseNotFoundException()
    ).flatMap {
      case false => throw new CollectionNotFoundException()
      case true =>
        val col = connection(dbName).collection[JSONCollection](colName)
        metadata(dbName, colName).map(md => (col, md))
    }


  def getCollectionInfo(dbName: String, colName: String): Future[CollectionInfo] = getCollection(dbName, colName) map {
    case (dbcoll, smd) if !smd.envelope.isEmpty =>
      (dbcoll, smd, FeatureTransformers.mkFeatureIndexingTranformer(smd.envelope, smd.level))
    case _ => throw new NoSpatialMetadataException(s"$dbName/$colName is not spatially enabled")
  }

  def getSpatialCollection(database: String, collection: String) = {
    val coll = connection(database).collection[JSONCollection](collection)
    metadata(database, collection).map(md => MongoSpatialCollection(coll, md))
  }

  def query(database: String, collection: String, spatialQuery: SpatialQuery, start: Option[Int] = None,
            limit: Option[Int] = None): Future[CountedQueryResult] =
    futureTimed("query-timer") {
        getSpatialCollection(database, collection) map {
          sc => sc.run(spatialQuery)
      } map {
        case enum if start.isDefined => enum &> Enumeratee.drop(start.get)
        case enum => enum
      } map {
        case enum if limit.isDefined => (None, enum &> Enumeratee.take(limit.get))
        case enum => (None, enum)
      }
    }


  def getMediaStore(database: String, collection: String): Future[MediaStore] = {

    existsCollection(database, collection).map(exists =>
      if (exists) new GridFS(connection(database), "fs." + collection)(BSONCollectionProducer)
      else throw CollectionNotFoundException()
    )
  }

  private def bson2String(bson: BSONValue): String = bson match {
    case bi: BSONObjectID => bi.stringify
    case bi: BSONString => bi.value
    case _ => bson.toString
  }

  def saveMedia(database: String, collection: String, producer: Enumerator[Array[Byte]], fileName: String,
                contentType: Option[String]): Future[Media] = {
    import reactivemongo.api.gridfs.Implicits._
    import reactivemongo.bson._

    getMediaStore(database, collection).flatMap { gridFs =>
      gridFs.save(producer, DefaultFileToSave(filename = Some(fileName), contentType = contentType, id = BSONObjectID.generate))
        .map(fr => Media(bson2String(fr.id), fr.md5))
    }
  }

  def getMedia(database: String, collection: String, id: String): Future[MediaReader] = {
    import reactivemongo.api.gridfs.Implicits._
    getMediaStore(database, collection).flatMap {
      gridFs => {
        Logger.info(s"Media object with id is searched: $id")
        gridFs.find(BSONDocument("_id" -> BSONObjectID(id)))
          .headOption
          .filter(_.isDefined)
          .flatMap(file =>
          (gridFs.enumerate(file.get) |>>> Iteratee.fold[Array[Byte], Array[Byte]](Array[Byte]())((accu, part) => accu ++ part))
            .map(binarydata => MediaReader(
              bson2String(file.get.id),
              file.get.md5,
              file.get.filename.getOrElse(""), file.get.length.toInt, file.get.contentType, binarydata))
          ).recover {
            case ex: Throwable =>
              Logger.warn(s"Media retrieval failed with exception ${ex.getMessage} of type: ${ex.getClass.getCanonicalName}")
              throw new MediaObjectNotFoundException()
          }
      }
    }
  }

  private def generateViewsCollName(collection: String) = s"fs.$collection.views"

  def getViewDefs(database: String, collection: String): Future[JSONCollection] =
    existsCollection(database, collection).map(exists =>
      if (exists) connection(database).collection[JSONCollection](generateViewsCollName(collection))
      else throw new CollectionNotFoundException()
    )

  /**
   * Saves a view for the specified database and collection.
   *
   * @param database the database for the view
   * @param collection the collection for the view
   * @param viewDef the view definition
   * @return eventually true if this save resulted in the update of an existing view, false otherwise
   */
  def saveView(database: String, collection: String, viewDef: JsObject): Future[Boolean] = {
    val result = for {
      c <- getViewDefs(database, collection)
      updateResult <- c.update(Json.obj("name" -> (viewDef \ "name").as[String]), viewDef,
                               commands.WriteConcern.Journaled
                               , upsert = true, multi = false)
      _ <- c.indexesManager.ensure(Index(Seq(("name", Ascending)), unique = true))
    } yield updateResult.nModified == 1


    result.andThen {
      case Success(_) => Logger.info(s"Writing view $viewDef for $collection succeeded")
      case Failure(t) => Logger.error(s"Writing metadata for $collection threw exception: ${t.getMessage}")
    }
    result
  }

  def getViews(database: String, collection: String): Future[List[JsObject]] =
    getViewDefs(database, collection) flatMap (_.find(Json.obj()).cursor[JsObject](ReadPreference.Primary).collect[List]())

  /**
   *
   * @param id  OID or name of the View
   */
  private def mkViewSelector(id: String) = Json.obj("$or" -> Json.arr(Json.obj("_id" -> id), Json.obj("name" -> id)))

  def getView(database: String, collection: String, id: String): Future[JsObject] =
    getViewDefs(database, collection) flatMap (_.find(mkViewSelector(id)).cursor[JsObject](ReadPreference.Primary).headOption.collect {
      case Some(js) => js
      case None => throw new ViewObjectNotFoundException()
    })

  def dropView(database: String, collection: String, id: String): Future[Boolean] =
    getViewDefs(database, collection)
      .flatMap(_.remove(mkViewSelector(id)))
      .map(le => if (le.n == 0) throw new ViewObjectNotFoundException() else true)

  override def writer(database: String, collection: String): FeatureWriter = new MongoWriter(this, database, collection)

  override def delete(database: String, collection: String, query: BooleanExpr): Future[Boolean] =
    getCollectionInfo(database, collection)
      .flatMap {
      case (coll, _, _) => coll.remove(expr2JsObject(query), commands.WriteConcern.Journaled)
    }.map(le => true)

  override def update(database: String, collection: String, query: BooleanExpr, updateSpec: JsObject): Future[Int] = {
    getCollectionInfo(database, collection)
      .flatMap {
      case (coll, metadata, reads) =>
        val updateDoc = getUpdateDoc(reads, updateSpec)
        coll.update(expr2JsObject(query), updateDoc, commands.WriteConcern.Journaled, upsert = false, multi = true)
    }.map(le => le.n)

  }

  override def insert(database: String, collection: String, json: JsObject): Future[Boolean]
  = upsert(database, collection, json)


  override def upsert(database: String, collection: String, json: JsObject): Future[Boolean] =
    getCollectionInfo(database, collection)
      .flatMap {
      case (coll, _, featureTransformer) =>
        val selectorDoc = Json.obj("id" -> (json \ "id").as[JsValue])
        val updateDoc = getUpdateDoc(featureTransformer, json)
        Logger.debug(s"Upserting document in collection $collection : ${Json.stringify(json)}")
        coll.update(selectorDoc, updateDoc, commands.WriteConcern.Journaled, upsert = true, multi = false)
    }.map(le => true)

  private def isUpdateDocWithOnlyOperators(updateDoc: JsObject): Boolean =
    updateDoc.keys.forall( k => k.startsWith( "$" ) )

  private def isUpdateDocWithOnlyFields(updateDoc: JsObject): Boolean =
    !updateDoc.keys.exists( k => k.startsWith( "$" ) )

  private def getUpdateDoc(trans: Reads[JsObject], updateDoc: JsObject): JsObject = {
    if (isUpdateDocWithOnlyOperators(updateDoc)) updateDoc
    else if (isUpdateDocWithOnlyFields(updateDoc)) {
      updateDoc.transform(trans) match {
        case s : JsSuccess[JsObject] => s.get
        case jsError: JsError =>
          throw InvalidParamsException(
            s"Error in update document \n: ${JsonHelper.JsValidationErrors2String(jsError.errors)}"
          )
      }
    } else
      throw InvalidParamsException("Only fields allowed in update document when upserting.")
  }

  override def createIndex(dbName: String, colName: String, indexDef: IndexDef): Future[Boolean] = ???

  override def getIndices(database: String, collection: String): Future[List[String]] = ???

  override def getIndex(database: String, collection: String, index: String): Future[IndexDef] = ???

  override def dropIndex(database: String, collection: String, index: String): Future[Boolean] = ???

  //Makes no sense for Mongo
  override def registerCollection(db: String, collection: String, md: Metadata): Future[Boolean] = sys.error("Not implemented")

}

