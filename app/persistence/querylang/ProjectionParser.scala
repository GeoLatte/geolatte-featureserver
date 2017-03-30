
package persistence.querylang

import org.parboiled2._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.util.{ Failure, Success, Try }

/**
 * Created by Karel Maesen, Geovise BVBA on 30/03/17.
 */
sealed trait Projection {
  self =>

  def reads: Reads[JsObject]

  def compose(other: Projection): Projection = new Projection {
    lazy val reads = (self.reads and other.reads) reduce
  }
}

object NoObjProjection extends Projection {

  lazy val reads = new Reads[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] = JsSuccess(Json.obj())
  }
}

case class ObjectProjection(path: JsPath) extends Projection {

  lazy val reads: Reads[JsObject] = path.json.pickBranch.orElse(path.json.put(JsNull))
}

case class InArrayProjection(arrayPath: JsPath, elementProjection: Projection) extends Projection {

  lazy val reads = arrayPath.json.copyFrom(
    arrayPath.json.pick[JsArray].map(js => JsArray(js.as(Reads.seq(elementProjection.reads))))
  )
}


sealed trait ProjExpr

case class SimplePropertyPath(path: JsPath) extends ProjExpr

object SimplePropertyPath {
  def fromString(str: String): SimplePropertyPath = new SimplePropertyPath(__ \ str)
  def combine(p1: SimplePropertyPath, p2: SimplePropertyPath): SimplePropertyPath = SimplePropertyPath(p1.path.compose(p2.path))
}

case class TraversablePropertyPath(path: SimplePropertyPath, inner: PropertyPathList) extends ProjExpr

case class PropertyPathList(paths: Seq[ProjExpr]) extends ProjExpr

object PropertyPathList {

  def fromPropertyExpr(p: ProjExpr): PropertyPathList = PropertyPathList(Seq(p))
  def addPropertyExpr(ps: PropertyPathList, p: ProjExpr): PropertyPathList = PropertyPathList(ps.paths ++ Seq(p))
}



class ProjectionParser(val input: ParserInput) extends Parser
    with StringBuilding {

  def InputLine = rule { ProjectionExpressionList ~ EOI }

  def ProjectionExpressionList: Rule1[PropertyPathList] = rule { (ProjectionExpression ~> PropertyPathList.fromPropertyExpr _) ~ zeroOrMore(ch(',') ~ WS ~ ProjectionExpression ~> PropertyPathList.addPropertyExpr _) }

  def ProjectionExpression = rule { TraversablePropertyExpr | SimpleProjectionExpression }

  def TraversablePropertyExpr = rule { SimpleProjectionExpression ~ ch('[') ~ ProjectionExpressionList ~ ch(']') ~> TraversablePropertyPath.apply _ }

  def SimpleProjectionExpression: Rule1[SimplePropertyPath] = rule { WS ~ PropertyEl ~ zeroOrMore(ch('.') ~ WS ~ PropertyEl ~> SimplePropertyPath.combine _) }

  def PropertyEl = rule { capture(NameString) ~> SimplePropertyPath.fromString _ ~ WS }

  //basic tokens
  def NameString = rule { !(ch(''') | ch('"')) ~ FirstNameChar ~ zeroOrMore(nonFirstNameChar) }

  def FirstNameChar = rule { CharPredicate.Alpha | ch('_') }

  def nonFirstNameChar = rule { CharPredicate.AlphaNum | ch('_') | ch('-') }

  def WS = rule { zeroOrMore(" " | "\n" | "\t") }

}

object ProjectionParser {

  val REQUIRED: Projection = ObjectProjection(__ \ 'type) compose
    ObjectProjection(__ \ 'geometry) compose
    ObjectProjection(__ \ 'id)

  //  def parse(s: String): Option[Projection] = collapse (
  //      for {
  //        projStr <- s.split(",").toList
  //        proj = parseEl(projStr)
  //      } yield proj
  //  ).map( proj => proj compose REQUIRED )

  def parse(str: String): Try[ProjExpr] = {
    val parser = new ProjectionParser(str)
    parser.InputLine.run() match {
      case s @ Success(_) => s
      case Failure(pe: ParseError) => Failure(new QueryParserException(parser.formatError(pe, showExpected = true, showPosition = true)))
      case f @ Failure(_) => f
    }
  }

  def mkProjection(paths: List[JsPath]): Option[Reads[JsObject]] = collapse(
    paths.map(ObjectProjection.apply)
  ).map(_.reads)

  def collapse(projs: List[Projection]): Option[Projection] =
    if (projs.isEmpty) None
    else {
      val r = projs.foldLeft[Projection](NoObjProjection) {
        (r1, proj) => r1 compose proj
      }
      Some(r)
    }

}

