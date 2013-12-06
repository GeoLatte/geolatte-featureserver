package utilities

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/9/13
 */
case class QueryParam[A](val name: String, binder:String => Option[A]) {

  type QueryStr = Map[String, Seq[String]]

  def bind(param: String): Option[A] = binder(param)

  def extract(implicit queryParams: QueryStr): Option[A] = getKeyCaseInsensitive(queryParams,name).flatMap( seq => binder(seq.head) )

  def extractOrElse( default: A)(implicit queryParams: QueryStr): A = extract.getOrElse(default)

  def getKeyCaseInsensitive(queryParams: QueryStr, key: String) : Option[Seq[String]] = {
    queryParams.filter { case (k,v) => k.equalsIgnoreCase(key) }.collect {
      case (k,v) => v
    }.headOption
  }

}



