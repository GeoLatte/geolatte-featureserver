package integration

import persistence.json.Gen
import persistence.json.Gen._
import play.api.libs.json._
import org.geolatte.geom.Envelope
import java.net.URLEncoder._
import scala.collection.Seq

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class QueryAPISpec extends InCollectionSpecification {

  def is = s2"""

      The FeatureCollection /query should:
        return 404 when the collection does not exist                              $e1
        return all elements when the collection does exist                         $e2

      The FeatureCollection /list should:
        return the objects contained within the specified bbox as json object      $e3
        respond to the START query-param                                           $e4
        respond to the LIMIT query-param                                           $e5
        support the SORT parameter                                                 $e5b
        support the SORT-DIRECTION parameter                                       $e5c
        support pagination                                                         $e6

      The FeatureCollection /query should:
        return the objects contained within the specified bbox as a stream          $e7
        support the PROJECTION parameter                                            $e8
        support the SORT parameter                                                  $e8b
        support the SORT-DIRECTION parameter                                        $e8c
        support the QUERY parameter                                                 $e9
        support the WITH-VIEW query-param                                             $e14
        support the WITH-VIEW query-param and a view with no projection clause        $e15
        BAD_REQUEST response code if the PROJECTION parameter is invalid              $e10
        BAD_REQUEST response code if the Query parameter is an invalid expression   $e11

      The FeatureCollection /distinct should:
        return distinct values                                                      $e14b

      General:
        Query parameters should be case insensitive                                 $e12

      The FeatureCollection /query in  CSV should:
        return the objects with all attributes within JSON Object tree              $e13


     Projection may specify fields not in inputJson (works only on postgresql)
        with Json output, fields are set to JsNull                                  $e16
        with CSV output, fields are empty strings                                   $e17

  """

  //import default values
  import UtilityMethods._

  //Generators for data
  val propertyObjGenerator = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3), "something" -> Gen.oneOf("else", "bad"))
  val nestedPropertyGenerator = propertyObjGenerator.map {
    jsObj => jsObj ++ Json.obj("nestedprop" -> Json.obj("nestedfoo" -> "bar"))
  }
  def lineStringGenerator(mc: String = "") = Gen.lineString(3)(mc)
  val idGen = Gen.id
  def geoJsonFeatureGenerator(mc: String = "") = Gen.geoJsonFeature(idGen, lineStringGenerator(mc), nestedPropertyGenerator)
  def gjFeatureArrayGenerator(mc: String = "", size: Int = 10) = Gen.geoJsonFeatureArray(geoJsonFeatureGenerator(mc), size)

  def e1 = getQuery(testDbName, "nonExistingCollection", "")(contentAsJsonStream).applyMatcher(_.status must equalTo(NOT_FOUND))

  def e2 = {
    val features = gjFeatureArrayGenerator(size = 1).sample.get
    withFeatures(testDbName, testColName, features) {
      getQuery(testDbName, testColName, "")(contentAsJsonStream).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSomeFeatures(features))
      )
    }
  }

  def e3 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeaturesInJson(featuresIn01)))
      )
  }

  def e4 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchTotalInJson(100)))
      )
  }

  def e5 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchTotalInJson(100)))
      )
  }

  def e5b = pending

  def e5c = pending

  def e6 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val buffer = scala.collection.mutable.ListBuffer[JsValue]()
        for (start <- 0 to 90 by 10) {
          buffer += getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> start, "limit" -> 10)).responseBody.get
        }
        collectFeatures(buffer.toSeq) must beFeatures(featuresIn01)
      }
  }

  def e7 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        getQuery(testDbName, testColName, Map("bbox" -> bbox))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(featuresIn01)
        }
      }
  }

  def e8 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(projectedFeatures)
        }
      }
  }

  def e8b = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val sort = "properties.foo"
        val projectedFeatures = project(projection)(featuresIn01)
        val sortedFeatures = JsArray(projectedFeatures.value.sortBy[String](jsValue => (jsValue \ "properties" \ "foo").as[String]))
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection, "sort" -> sort))(contentAsJsonStream).applyMatcher {
          res =>
            {
              res.responseBody must beSomeFeatures(sortedFeatures, true)
            }
        }
      }
  }

  def e8c = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val sort = "properties.foo"
        val sortdir = "DESC"
        val projectedFeatures = project(projection)(featuresIn01)
        val sortedFeatures = JsArray(projectedFeatures.value.sortBy[String](jsValue => (jsValue \ "properties" \ "foo").as[String]).reverse)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection, "sort" -> sort, "sort-direction" -> sortdir))(
          contentAsJsonStream
        ).applyMatcher {
          res =>
            {
              res.responseBody must beSomeFeatures(sortedFeatures, true)
            }
        }
      }
  }

  def e9 = withTestFeatures(100, 200) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val picksFoo = (__ \ "properties" \ "foo").json.pick
        val filteredFeatures = JsArray(
          featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
        )
        val queryObj = "properties.foo='bar1'"
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "query" -> encode(queryObj, "UTF-8")))(contentAsJsonStream) applyMatcher {
          res => res.responseBody must beSomeFeatures(filteredFeatures)
        }
      }
  }

  def e14 = withTestFeatures(100, 200) {
    val projection = "properties.foo,properties.num"
    val jsInViewDef = Json.obj("query" -> JsString("properties.foo = 'bar1'"), "projection" -> Json.arr("properties.foo", "properties.num"))
    loadView(testDbName, testColName, "view-1", jsInViewDef)

    (bbox: String, featuresIn01: JsArray) => {
      val picksFoo = (__ \ "properties" \ "foo").json.pick
      val filteredFeatures = JsArray(
        featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
      )
      val projected = project(projection)(filteredFeatures)
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "with-view" -> "view-1"))(contentAsJsonStream) applyMatcher {
        res => res.responseBody must beSomeFeatures(projected)
      }
    }
  }

  def e14b = withTestFeatures(100, 200) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val picksFoo = (__ \ "properties" \ "foo").json.pick
        val distinctFoo = featuresIn01.value.flatMap(jsv => jsv.asOpt(picksFoo)).map(_.as[String]).distinct.toSeq

        getDistinct(testDbName, testColName, Map("bbox" -> bbox, "projection" -> "properties.foo"))(contentAsJsonStream) applyMatcher { res =>
          val strings = res.responseBody.map(_.value.flatMap(_.as[List[String]])).getOrElse(Nil)
          strings must containTheSameElementsAs(distinctFoo)
        }
      }
  }

  def e15 = withTestFeatures(100, 200) {
    val jsInViewDef = Json.obj("query" -> JsString("properties.foo = 'bar1'"))
    loadView(testDbName, testColName, "view-2", jsInViewDef)

    (bbox: String, featuresIn01: JsArray) => {
      val picksFoo = (__ \ "properties" \ "foo").json.pick
      val filteredFeatures = JsArray(
        featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
      )
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "with-view" -> "view-2"))(contentAsJsonStream) applyMatcher {
        res => res.responseBody must beSomeFeatures(filteredFeatures)
      }
    }
  }

  def e10 = getQuery(testDbName, testColName, Map("projection" -> "fld["))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e11 = getQuery(testDbName, testColName, Map("query" -> """ (foo = 1 """))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e12 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val lcResponse = getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 5)).responseBody.get
        val ucResponse = getList(testDbName, testColName, Map("BBOX" -> bbox, "LIMIT" -> 5)).responseBody.get
        lcResponse must equalTo(ucResponse)
      }
  }

  def e13 = withTestFeatures(3, 6) {
    (bbox: String, featuresIn01: JsArray) =>
      getQuery(testDbName, testColName, Map("bbox" -> bbox))(contentAsStringStream).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeaturesInCsv("id,geometry-wkt,foo,nestedprop.nestedfoo,num,something")))
      )
  }

  def e16 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.bar"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(projectedFeatures)
        }
      }
  }

  def e17 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.bar"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsStringStream).applyMatcher {
          res => res.responseBody must beSome(matchFeaturesInCsv("id,geometry-wkt,bar,foo"))
        }
      }
  }

  def withTestFeatures[T](sizeInsideBbox: Int, sizeOutsideBbox: Int)(block: (String, JsArray) => T) = {
    val (featuresIn01, allFeatures) = gjFeatureArrayGenerator("01", sizeInsideBbox)
      .flatMap(f1 => gjFeatureArrayGenerator("1", sizeOutsideBbox).map(f2 => (f1, f2 ++ f1))).sample.get
    val env: Envelope = "01"
    val bbox = s"${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY}"
    withFeatures(testDbName, testColName, allFeatures) {
      block(bbox, featuresIn01)
    }
  }

  //hardcoded projection parameter for now
  def project(projection: String)(features: JsArray) = {
    import play.api.libs.functional.syntax._
    import play.api.libs.functional._

    val fields = List("foo", "num", "something", "nestedprop")
    val projectionFields = projection.split(",").map(fp => fp.split("\\.")(1))
    val fieldsToPrune = fields.filterNot(f => projectionFields.contains(f))
    val fieldsToAdd = projectionFields.filterNot(f => fields.contains(f))

    val pruner: Reads[JsObject] = {

      val p1 = fieldsToPrune.foldLeft((__ \ "properties" \ "id").json.prune) {
        (p, field) => p andThen ((__ \ "properties" \ field).json.prune)
      }

      fieldsToAdd.foldLeft(p1) {
        (p, field) => p andThen (__ \ "properties").json.update((__ \ "bar").json.put(JsNull))
      }

    }

    JsArray(
      for {
        f <- features.value
        pruned = f.transform(pruner)
      } yield pruned.asOpt.get
    )
  }

  def collectFeatures(listOfResponses: Seq[JsValue]) =
    listOfResponses.foldLeft(JsArray())((state, elem) => (elem \ "features").asOpt[JsArray] match {
      case Some(arr) => state ++ arr
      case _         => state
    })

}
