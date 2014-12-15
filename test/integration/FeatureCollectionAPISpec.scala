package integration

import nosql.json.Gen
import nosql.json.Gen._
import play.api.libs.json._
import play.api.http.Status._
import org.geolatte.geom.Envelope

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class FeatureCollectionAPISpec extends InCollectionSpecification {


  def is = s2"""
                                                                                  ${section("mongodb")}
     The FeatureCollection /download should:
       return 404 when the collection does not exist                              $e1
       return all elements when the collection does exist                         $e2



     The FeatureCollection /list should:
       return the objects contained within the specified bbox as json object      $e3
       respond to the start query-param                                           $e4 ${tag("postgresql")}
       respond to the limit query-param                                           $e5
       support pagination                                                         $e6

     The FeatureCollection /query should:
      return the objects contained within the specified bbox as a stream          $e7
      support the PROJECTION parameter                                            $e8
      support the QUERY parameter                                                 $e9
      BAD_REQUEST response code if the PROJECTION parameter is empty or invalid   $e10
      BAD_REQUEST response code if the Query parameter is invalid JSON            $e11

     General:
       Query parameters should be case insensitive                                $e12


     The FeatureCollection /query in  CSV should:
        return the objects with all attributes within JSON Object tree            $e13
                                                                                  ${section("mongodb")}

  """

  //import default values
    import UtilityMethods._
    import RestApiDriver._

    //Generators for data
    val propertyObjGenerator = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3), "something" -> Gen.oneOf("else", "bad"))
    val nestedPropertyGenerator = propertyObjGenerator.map {
      jsObj => jsObj ++ Json.obj("nestedprop" -> Json.obj("nestedfoo" -> "bar"))
    }
    def lineStringGenerator(mc: String = "") = Gen.lineString(3)(mc)
    val idGen = Gen.id
    def geoJsonFeatureGenerator(mc: String = "") = Gen.geoJsonFeature(idGen, lineStringGenerator(mc), nestedPropertyGenerator)
    def gjFeatureArrayGenerator(mc: String = "", size: Int = 10) = Gen.geoJsonFeatureArray(geoJsonFeatureGenerator(mc), size)

  def e1 = getDownload(testDbName, "nonExistingCollection").applyMatcher( _.status must equalTo(NOT_FOUND))

  def e2 = {
    val features = gjFeatureArrayGenerator(size = 1).sample.get
    withFeatures(testDbName, testColName, features){
      getDownload(testDbName, testColName).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSomeFeatures(features))
      )
    }
  }

  def e3 = withTestFeatures(10, 10) {
      (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome( matchFeaturesInJson(featuresIn01)))
      )
    }


  def e4 = withTestFeatures(100, 10){
      (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome( (js: JsValue) =>
          (js must matchTotalInJson(100)) and (js must matchCountInJson(90))))
      )
    }

  def e5 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) => getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 10)).applyMatcher(
      res => (res.status must equalTo(OK)) and (res.responseBody must beSome((js: JsValue) =>
        (js must matchTotalInJson(100)) and (js must matchCountInJson(10))))
    )
  }

  def e6 = withTestFeatures(100, 10) {
      (bbox: String, featuresIn01: JsArray) => {
        val buffer = scala.collection.mutable.ListBuffer[JsValue]()
        for (start <- 0 to 90 by 10) {
          buffer += getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> start, "limit" -> 10)).responseBody.get
        }
        collectFeatures(buffer.toSeq) must beFeatures(featuresIn01)
      }
    }

  def e7 = withTestFeatures(10,10) {
    (bbox: String, featuresIn01: JsArray) => {
      getQuery(testDbName, testColName, Map("bbox"-> bbox))(contentAsJsonStream).applyMatcher {
        res => res.responseBody must beSomeFeatures(featuresIn01)
      }
    }
  }

  def e8 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) => {
      val projection = "properties.foo,properties.num"
      val projectedFeatures = project(featuresIn01)
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsJsonStream).applyMatcher {
        res => res.responseBody must beSomeFeatures(projectedFeatures)
      }
    }
  }

  def e9 = withTestFeatures(100, 200) {
    (bbox: String, featuresIn01: JsArray) => {
      val picksFoo = ( __ \ "properties" \ "foo").json.pick
      val filteredFeatures = JsArray(
        featuresIn01.value.filter( jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
      )
      val queryObj = Json.obj("properties.foo" -> "bar1")
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "query" -> queryObj))(contentAsJsonStream) applyMatcher {
        res => res.responseBody must beSomeFeatures(filteredFeatures)
      }
    }
  }

  def e10 = getQuery(testDbName, testColName, Map("projection" -> ""))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e11 = getQuery(testDbName, testColName, Map("query" -> """{"foo": 1"""))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e12 =  withTestFeatures(10, 10) {
        (bbox: String, featuresIn01: JsArray) => {
          val lcResponse = getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 5)).responseBody.get
          val ucResponse = getList(testDbName, testColName, Map("BBOX" -> bbox, "LIMIT" -> 5)).responseBody.get
          lcResponse must equalTo(ucResponse)
        }
      }
  
  def e13 = withTestFeatures(3, 6) {
    (bbox: String, featuresIn01: JsArray) => getQuery(testDbName, testColName, Map("bbox" -> bbox))(contentAsStringStream).applyMatcher(
      res => (res.status must equalTo(OK)) and (res.responseBody must beSome( matchFeaturesInCsv("_id,geometry-wkt,foo,num,something,nestedprop.nestedfoo")))
    )
  }

  def withTestFeatures[T](sizeInsideBbox: Int, sizeOutsideBbox: Int)( block: (String, JsArray) => T) = {
    val (featuresIn01, allFeatures) = gjFeatureArrayGenerator("01", sizeInsideBbox)
          .flatMap(f1 => gjFeatureArrayGenerator("1", sizeOutsideBbox).map(f2 => (f1, f2 ++ f1))).sample.get
        val env: Envelope = "01"
        val bbox = s"${env.getMinX},${env.getMinY},${env.getMaxX},${env.getMaxY}"
        withFeatures(testDbName, testColName, allFeatures) {
          block(bbox, featuresIn01)
        }
  }

  //hardcoded projection parameter for now
  def project(features: JsArray) = {
    import play.api.libs.functional.syntax._
    val pruner : Reads[JsObject] = (__ \ "id").json.prune andThen
      ( __ \ "properties" \ "something").json.prune andThen
      ( __ \ "properties" \ "nestedprop").json.prune
    JsArray(
      for (f <- features.value) yield f.transform(pruner).asOpt.get
    )
  }
  
  def collectFeatures(listOfResponses: Seq[JsValue]) =
    listOfResponses.foldLeft(JsArray())((state, elem) => (elem \ "features").asOpt[JsArray] match {
      case Some(arr) => state ++ arr
      case _ => state
    })


}
