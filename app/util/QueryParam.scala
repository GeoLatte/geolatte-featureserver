package util

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/9/13
 */
case class QueryParam[A](val name: String, binder:String => Option[A]) {
  def bind(param: String): Option[A] = binder(param)
}

object QueryParam {

  def make[A](name:String , binder: String => Option[A]) = new QueryParam(name, binder)

}


