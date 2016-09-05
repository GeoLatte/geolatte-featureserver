package nosql.postgresql


//import com.github.tminglei.slickpg._
//import play.api.libs.json.{JsValue, Json}
//
//
//trait NoSqlPGDriver extends ExPostgresProfile
//  with PgArraySupport
//  with PgJsonSupport
//  with PgNetSupport
//  with PgLTreeSupport
//  with PgRangeSupport
//  with PgHStoreSupport
//  with PgPlayJsonSupport
//  with PgSearchSupport {
//
//  override val pgjson = "jsonb"
//  ///
//  override val api = new API with ArrayImplicits
//    with SimpleJsonImplicits
//    with NetImplicits
//    with LTreeImplicits
//    with RangeImplicits
//    with HStoreImplicits
//    with SearchImplicits
//    with PlayJsonImplicits
//    with SearchAssistants {
//    implicit val strListTypeMapper = new SimpleArrayJdbcType[String]("text").to(_.toList)
//    implicit val playJsonArrayTypeMapper =
//      new AdvancedArrayJdbcType[JsValue](pgjson,
//        (s) => utils.SimpleArrayUtils.fromString[JsValue](Json.parse(_))(s).orNull,
//        (v) => utils.SimpleArrayUtils.mkString[JsValue](_.toString())(v)
//      ).to(_.toList)
//  }
//}
//
//object NoSqlPGDriver extends NoSqlPGDriver