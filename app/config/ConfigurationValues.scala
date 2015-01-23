package config

import java.io.File

import nosql.mongodb.MongoDBRepository
import play.api.{Mode, DefaultApplication}

import scala.language.implicitConversions
import play.api.Play._

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 7/23/13
 */

object ConfigurationValues {

  import play.api.Play.current

  //configuration keys
  val MONGO_CONNECTION_STRING_KEY = "fs.mongodb"
  val PG_CONNECTION_URL_KEY = "fs.postgresql"
  val MONGO_SYSTEM_DB_KEY = "fs.system.db"
  val MAXIMUM_RESULT_SIZE_KEY= "max-collection-size"

  // Default values
  val DEFAULT_CREATED_DBS_COLLECTION = "createdDatabases"
  val DEFAULT_CREATED_DB_PROP = "db"
  val DEFAULT_SYS_DB = "featureServerSys"
  val DEFAULT_DB_HOST = "localhost"
  val DEFAULT_PG_CONNECTION_URL = "postgresql:localhost/5432"
  val DEFAULT_MAXIMUM_RESULT_SIZE = 10000

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

  /**
   * Provides acces to the current Play application.
   *
   * If Play itself hasn't started, we lazily create a default application so that all
   * configuration information is accessible here.
   */
  lazy val currentApp = maybeApplication.getOrElse(
    new DefaultApplication(new File("."), this.getClass.getClassLoader, None, Mode.Dev)
  )

  def getConfigString(key: String, default: String) : String =
    currentApp.configuration.getString(key) match {
    case Some(value) => value
    case None => default
  }

  import scala.collection.JavaConversions._
  def getConfigStringList(key: String, default: List[String]) : List[String] =
    currentApp.configuration.getStringList(key) match {
    case Some(value) => value.toList
    case None => default
  }

  def getConfigInt(key: String, default: Int) : Int =
  currentApp.configuration.getInt(key) match {
    case Some(value) => value
    case None => default
  }

  val configuredRepository = getConfigString("fs.db", "mongodb")

  val MongoConnnectionString = getConfigStringList(MONGO_CONNECTION_STRING_KEY, List[String](DEFAULT_DB_HOST))

  val PgConnectionString: String = getConfigString(PG_CONNECTION_URL_KEY, DEFAULT_PG_CONNECTION_URL)


  val MongoSystemDB = getConfigString(MONGO_SYSTEM_DB_KEY, DEFAULT_SYS_DB)

  val MaxReturnItems = getConfigInt(MAXIMUM_RESULT_SIZE_KEY, DEFAULT_MAXIMUM_RESULT_SIZE)

  /**
   * The separator to use between any two Json strings when streaming JSON
   */
  val jsonSeparator = "\n"

}
