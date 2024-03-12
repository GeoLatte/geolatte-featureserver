package persistence.postgresql

import persistence.FldSortSpec

/**
 * Created by Karel Maesen, Geovise BVBA on 06/11/2019.
 */
trait PGExpression {

  //TODO -- change parameter flds to Field Type
  def fldSpecToComponents(flds: String): Seq[String] = flds.split("\\.").toIndexedSeq

  protected def quoteField(str: String) = s"'$str'"

  def intersperse[A](a: Seq[A], b: Seq[A]): Seq[A] = a match {
    case first +: rest => first +: intersperse(b, rest)
    case _             => b
  }

  def intersperseOperators(flds: Seq[String], ops: String, lastOp: Option[String] = None) = {
    if (flds.size <= 1) flds.headOption.getOrElse("")
    else {
      val operators = lastOp match {
        case Some(lp) => (0 until (flds.length - 2)).map(_ => ops) :+ lp
        case _        => (0 until (flds.length - 1)).map(_ => ops)
      }
      intersperse(flds, operators).mkString("")
    }
  }

  def jsonFieldSelector(paths: Seq[String]): String =
    paths.map(jsonFieldSelector).mkString(",")

  def jsonFieldSelector(path: String): String = {
    val quotedFields = fldSpecToComponents(path).map(quoteField)
    val flds = "json" +: quotedFields
    s"( ${intersperseOperators(flds, "->", Some("->>"))} )"
  }

  def fldSortSpecToSortExpr(spec: FldSortSpec): String = {
    s"${jsonFieldSelector(spec.fld)} ${spec.direction.toString}"
  }

}
