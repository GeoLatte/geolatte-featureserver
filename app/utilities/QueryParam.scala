package utilities

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/9/13
 */
case class QueryParam[A](val name: String, binder:String => Option[A]) {

  type QueryStr = Map[String, Seq[String]]

  def bind(param: String): Option[A] = binder(param)

  def extract(implicit queryParams: QueryStr): Option[A] = queryParams.get(name).flatMap( seq => binder(seq.head) )

  def extractOrElse( default: A)(implicit queryParams: QueryStr): A = extract.getOrElse(default)

}



