package controllers

import play.api.mvc.RequestHeader

/**
 * @author Karel Maesen, Geovise BVBA
 * creation-date: 3/9/13
 */
case class QueryParam[A](val name: String, binder: String => Option[A]) {

  type QueryStr = Map[String, Seq[String]]

  def bind(param: String): Option[A] = binder(param)

  def value(implicit req: RequestHeader): Option[A] = getKeyCaseInsensitive(req.queryString, name).flatMap(seq => binder(seq.head))

  private def getKeyCaseInsensitive(queryParams: QueryStr, key: String): Option[Seq[String]] = {
    queryParams.filter { case (k, v) => k.equalsIgnoreCase(key) }.collect {
      case (k, v) => v
    }.headOption
  }

}

