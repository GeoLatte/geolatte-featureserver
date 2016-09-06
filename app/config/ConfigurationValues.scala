package config

import scala.language.implicitConversions

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 7/23/13
 */

object ConfigurationValues {

  import play.api.Play.current

  //configuration keys
  private val MONGO_CONNECTION_STRING_KEY = "fs.mongodb.url"
  private val PG_CONNECTION_URL_KEY = "fs.postgresql.url"
  private val MONGO_SYSTEM_DB_KEY = "fs.system.db"
  private val MAXIMUM_RESULT_SIZE_KEY= "max-collection-size"
  private val PG_MAXOBJECTS_KEY = "fs.postgresql.max_objects"
  private val PG_MAXIDLE_KEY = "fs.postgresql.max-idle"
  private val PG_MAXQUEUESIZE_KEY= "fs.postgresql.max-queuesize"
  private val PG_VALIDATION_INTERVAL_KEY = "fs.postgresql.validation_interval"

  // Default values
  val DEFAULT_CREATED_DBS_COLLECTION = "createdDatabases" //TODO -- should be private
  val DEFAULT_CREATED_DB_PROP = "db"
  private val DEFAULT_SYS_DB = "featureServerSys"
  private val DEFAULT_DB_HOST = "localhost"
  private val DEFAULT_PG_CONNECTION_URL = "postgresql:localhost/5432"

  //PGSQL connection pool configuration
  private val DEFAULT_PG_MAXOBJECTS = 10
  private val DEFAULT_PG_MAXIDLE = 4
  private val DEFAULT_PG_MAXQUEUESIZE= 10
  private val DEFAULT_PG_VALIDATION_INTERVAL = 5000

  class ConstantsEnumeration extends Enumeration {
    def unapply (s: String): Option[Value] = values.find(_.toString.toUpperCase == s.toUpperCase)
  }

  object Format extends ConstantsEnumeration {
    type Formats = Value
    val JSON, CSV = Value
    def stringify(v: Value) : String = v.toString.toLowerCase
  }

  object Version extends ConstantsEnumeration {
    type Versions = Value
    val v1_0 = Value
    def default = v1_0
    def stringify(v : Value) : String = v.toString.replace("_", ".").drop(1)
    override def unapply (s: String): Option[Value] = values.find(_.toString == "v" + s.replace(".","_"))
  }

//  /**
//   * Provides acces to the current Play application.
//   *
//   * If Play itself hasn't started, we lazily create a default application so that all
//   * configuration information is accessible here.
//   */
//  lazy val currentApp = maybeApplication.getOrElse(
//    new DefaultApplication(new File("."), this.getClass.getClassLoader, None, Mode.Dev)
//  )

  def getConfigString(key: String, default: String) : String =
    current.configuration.getString(key) match {
    case Some(value) => value
    case None => default
  }

  import scala.collection.JavaConversions._
  def getConfigStringList(key: String, default: List[String]) : List[String] =
    current.configuration.getStringList(key) match {
    case Some(value) => value.toList
    case None => default
  }

  def getConfigInt(key: String, default: Int) : Int =
  current.configuration.getInt(key) match {
    case Some(value) => value
    case None => default
  }

  val configuredRepository = getConfigString("fs.db", "mongodb")

  val MongoConnnectionString = getConfigStringList(MONGO_CONNECTION_STRING_KEY, List[String](DEFAULT_DB_HOST))

  val PgConnectionString: String = getConfigString(PG_CONNECTION_URL_KEY, DEFAULT_PG_CONNECTION_URL)

  val PgMaxIdle = getConfigInt(PG_MAXIDLE_KEY, DEFAULT_PG_MAXIDLE)

  val PgMaxObjects = getConfigInt(PG_MAXOBJECTS_KEY, DEFAULT_PG_MAXOBJECTS)

  val PgMaxQueueSize = getConfigInt(PG_MAXQUEUESIZE_KEY, DEFAULT_PG_MAXQUEUESIZE)

  val PgMaxValidationinterval = getConfigInt(PG_VALIDATION_INTERVAL_KEY, DEFAULT_PG_VALIDATION_INTERVAL)

  val MongoSystemDB = getConfigString(MONGO_SYSTEM_DB_KEY, DEFAULT_SYS_DB)

  /**
   * The separator to use between any two chunks when using chunked response streaming
   */
  val chunkSeparator = "\n"

}
