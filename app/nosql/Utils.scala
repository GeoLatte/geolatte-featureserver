package nosql

import Exceptions.InvalidRequestException
import com.github.mauricio.async.db.{QueryResult, RowData}
import org.geolatte.geom.ByteBuffer
import org.geolatte.geom.codec.Wkb
import play.api.Logger
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._

import scala.concurrent.Future
import scala.util.{Success, Try}

/**
  * Created by Karel Maesen, Geovise BVBA on 08/04/16.
  */
object Utils {

  import scala.concurrent.ExecutionContext.Implicits.global

  def int(v: Any) : Int = v.asInstanceOf[Int]
  def string(v: Any) : String = v.asInstanceOf[String]
  def double(v: Any) : Double = v.asInstanceOf[Double]
  def boolean(v: Any) : Boolean = v.asInstanceOf[Boolean]
  def json(v: Any) : JsValue = Json.parse( v.asInstanceOf[String])

  def withWarning[T](msg: String)(t: => T) = {
    Logger.warn(msg)
    t
  }

  def withInfo[T](msg: String)(t: => T) = {
    Logger.info(msg)
    t
  }

  def withError[T](msg: String)(t: => T) = {
    Logger.error(msg)
    t
  }

  def withDebug[T](msg: String)(t: => T) = {
    Logger.debug(msg)
    t
  }

  def withTrace[T](msg: String)(t: => T) = {
    Logger.trace(msg)
    t
  }

  def debug[T](t: => T) : T= {
    println("DEBUG: " + t.toString)
    t
  }

  //TODO -- replace with Cats??
  trait Foldable[B] {
    //by name argument in second position is vital to have proper "serializing" behavior
    def combine(b1: B, b2: => B) : B
    def unit: B
  }

  implicit object BooleanFutureFoldable extends Foldable[Future[Boolean]] {
    override def combine( b1 : Future[Boolean], b2: => Future[Boolean]): Future[Boolean] = b1.flatMap( bb1 => b2.map( _ && bb1))
    override def unit: Future[Boolean] = Future.successful(true)
  }

  def sequence[M,B](items: List[M])(f: M => B)(implicit ev: Foldable[B]): B =
    items.foldLeft( ev.unit ) { (res, m) => ev.combine(res, f(m)) }


  def toFuture[T](opt: Option[T], failure: Throwable) : Future[T] = opt match {
    case Some(t) => Future.successful(t)
    case _      =>  Future.failed(failure)
  }

  def toFuture[T](result:JsResult[T]) : Future[T] = result match {
    case JsSuccess(t, _) => Future.successful(t)
    case JsError(errs) => Future.failed(new InvalidRequestException(Json.stringify(JsError.toFlatJson(errs))))
  }

  implicit class FuturableOption[T](opt: Option[T]) {
    def future(default: Throwable) : Future[T] = toFuture(opt, default)
  }

  implicit class FuturableJsResult[T](result: JsResult[T]) {
    def future: Future[T] = toFuture(result)
  }


}

object JsonUtils {

  def toJsValue(value: Any) : JsValue = value match {
    case v if v == null => JsNull
    case v : String => JsString(v)
    case v : Int => JsNumber(v)
    case v : Long => JsNumber(v)
    case b : Boolean => JsBoolean(b)
    case f : Float => JsNumber(BigDecimal(f))
    case d : Double => JsNumber(BigDecimal(d))
    case b : BigDecimal => JsNumber(b)
    case e => Utils.withTrace(s"Converting value $e (${e.getClass.getCanonicalName}) by toString method") (
      Try { JsString(e.toString) }.toOption.getOrElse(JsNull)
    )
  }

  def toGeoJson(idColumn: String, geomCol: String, propertyMap: Map[String, Any]) : JsObject = {
    val props = for {
      (key, value) <- propertyMap if key != idColumn && key != geomCol && key != "__geojson"
    } yield (key -> toJsValue(value))

    val flds : Seq[(String, JsValue)]  = Seq(
      "id" -> toJsValue(propertyMap(idColumn)),
      "geometry" -> Try{
        Json.parse(Utils.string(propertyMap("__geojson")))
        }.getOrElse(Utils.withWarning(s"Failed to parse ${Utils.string(propertyMap("__geojson"))}")(JsNull)),
      "type" -> JsString("Feature"),
      "properties" -> JsObject(props.toSeq)
    )
    JsObject(flds)
  }

  def toJson(text : String)(implicit reads : Reads[JsObject]) : Option[JsObject] =
    Json
      .parse(text)
      .asOpt[JsObject](reads)
}

object QueryResultUtils {

  def first[T](rowF : RowData => Option[T])(qr: QueryResult) : Option[T] = qr.rows match {
    case Some(rs) if rs.nonEmpty => rowF(rs.head)
    case _ => None
  }

  def toList[T](qr: QueryResult)(rowF : RowData => Option[T]): List[T] = qr.rows match {
    case Some(rs) =>
      rs.map(rowF).collect{case Some(v) => v}.toList
    case _ => List[T]()
  }

  def toProjectedJsonList(qr: QueryResult, optProj : Option[Reads[JsObject]]) : List[JsObject] = {
    def json(row : RowData) : Option[JsObject] = JsonUtils.toJson(row(0).asInstanceOf[String])
    def project(reads : Reads[JsObject])(jsOpt : Option[JsObject]) : Option[JsObject] = jsOpt flatMap (_.asOpt(reads))
    val transformFunc = optProj match {
      case Some(reads) => json _ andThen project(reads)
      case _ => json _
    }
    toList(qr)(transformFunc)
  }

  def toMap(colnames: IndexedSeq[String])(row: RowData): Map[String, Any] =
    colnames.map { c => (c,row(c)) } toMap

}

