package config

import scala.language.implicitConversions

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 7/23/13
 */

object ConfigurationValues {

  import play.api.Play.current

  // Default values
  private val DEFAULT_CREATED_DBS_COLLECTION = "createdDatabases"
  private val DEFAULT_CREATED_DB_PROP = "db"
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

  /**
   * The separator to use between any two chunks when using chunked response streaming
   */
  val chunkSeparator = "\n"

}
