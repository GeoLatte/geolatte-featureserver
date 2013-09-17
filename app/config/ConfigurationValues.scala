package config

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 7/23/13
 */
object ConfigurationValues {


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
   * The separator to use between any two Json strings when streaming JSON
   */
  val jsonSeparator = "\n"

}
