
package persistence.querylang

import org.parboiled2._
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.util.{ Failure, Success, Try }

/**
 *
 * Created by Karel Maesen, Geovise BVBA on 30/03/17.
 */
sealed trait Projection {
  def reads: Reads[JsObject]
}

case class SimpleProjection(path: JsPath) extends Projection {
  lazy val reads: Reads[JsObject] = path.json.pickBranch.orElse(path.json.put(JsNull))
}

object SimpleProjection {
  def fromString(str: String): SimpleProjection = new SimpleProjection(__ \ str)
  def combine(p1: SimpleProjection, p2: SimpleProjection): SimpleProjection = SimpleProjection(p1.path.compose(p2.path))
}

case class TraversableProjection(simplePath: SimpleProjection, inner: ProjectionList) extends Projection {
  lazy val reads = simplePath.path.json.copyFrom(
    simplePath.path.json.pick[JsArray].map(js => JsArray(js.as(Reads.seq(inner.reads)))).orElse(Reads[JsArray] { _ => JsSuccess(JsArray()) })
  )
}

case class ProjectionList(paths: Seq[Projection]) extends Projection {

  def withPrefix(pre: Seq[String]) = ProjectionList(pre.map(s => SimpleProjection(__ \ s)) ++ this.paths)

  lazy val nullReads = new Reads[JsObject] {
    override def reads(json: JsValue): JsResult[JsObject] = JsSuccess(Json.obj())
  }

  lazy val reads: Reads[JsObject] =
    paths.foldLeft(nullReads)((r1, p) => (r1 and p.reads) reduce)

  def isEmpty = paths.isEmpty

  def ++(o: ProjectionList) = new ProjectionList(this.paths ++ o.paths)
}

object ProjectionList {

  def fromPropertyExpr(p: Projection): ProjectionList = ProjectionList(Seq(p))
  def addPropertyExpr(ps: ProjectionList, p: Projection): ProjectionList = ProjectionList(ps.paths ++ Seq(p))
}

/**
 * A Parser for the Projection expresions.
 *
 * <p> The Projection syntax is : </p>
 *
 *   <ul>
 *     <li> &lt;ProjectionExpressionList&gt; ::=  &lt;ProjectionExpression&gt;{, &lt;ProjectionExpression} </li>
 *     <li> &lt;ProjectionExpression&gt;     ::= &lt;TraversableExpr&gt; | &lt;SimpleExpr&gt; </li>
 *     <li> &lt;TraversableExpr&gt           ::= &lt;SimpleExpr&gt;"[&lt;ProjectionExpressionList&gt;"]" </li>
 *     <li> &lt;SimpleExpr&gt;               ::= &lt;pathEl&gt;{"."&lt;pathEl&gt;] </li>
 *       <li> &lt;pathEl&gt;                 ::= <i>sequence of characters, valid for a json property key</i>
 *   </ul>
 *
 * <p>In a <code>TraversableExpr</code>, the first part should point to an array-valued property. The expressions within the square brackets
 * will be applied to every element within the array.
 * </p>
 * @param input the ParserInput
 */
class ProjectionParser(val input: ParserInput) extends Parser
    with StringBuilding {

  def InputLine = rule { ProjectionExpressionList ~ EOI }

  def ProjectionExpressionList: Rule1[ProjectionList] = rule { (ProjectionExpression ~> ProjectionList.fromPropertyExpr _) ~ zeroOrMore(ch(',') ~ WS ~ ProjectionExpression ~> ProjectionList.addPropertyExpr _) }

  def ProjectionExpression = rule { TraversableExpr | SimpleExpr }

  def TraversableExpr = rule { SimpleExpr ~ ch('[') ~ ProjectionExpressionList ~ ch(']') ~> TraversableProjection.apply _ }

  def SimpleExpr: Rule1[SimpleProjection] = rule { WS ~ PropertyEl ~ zeroOrMore(ch('.') ~ WS ~ PropertyEl ~> SimpleProjection.combine _) }

  def PropertyEl = rule { capture(NameString) ~> SimpleProjection.fromString _ ~ WS }

  //basic tokens
  def NameString = rule { !(ch(''') | ch('"')) ~ FirstNameChar ~ zeroOrMore(nonFirstNameChar) }

  def FirstNameChar = rule { CharPredicate.Alpha | ch('_') }

  def nonFirstNameChar = rule { CharPredicate.AlphaNum | ch('_') | ch('-') }

  def WS = rule { zeroOrMore(" " | "\n" | "\t") }

}

object ProjectionParser {

  def parse(str: String): Try[ProjectionList] = {
    val parser = new ProjectionParser(str)
    parser.InputLine.run() match {
      case s @ Success(_) => s
      case Failure(pe: ParseError) => Failure(new QueryParserException(parser.formatError(pe, new ErrorFormatter(showExpected = true, showPosition = true))))
      case f @ Failure(_) => f
    }
  }

  def mkProjection(paths: List[JsPath]): Option[Reads[JsObject]] = {
    val ppl = ProjectionList(
      paths.map(SimpleProjection.apply)
    )
    if (ppl.isEmpty) None
    else Some(ppl.reads)
  }

}

