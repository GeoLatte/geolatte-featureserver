package utilities

import scala.util.Random
import org.geolatte.geom._
import org.geolatte.geom.crs.CrsId
import nosql.json.GeometryReaders._
import play.api.libs.json._

/**
 * Test generator for (linestring) features into a collection
 * @author Karel Maesen, Geovise BVBA, 2013
 */
case class TestDataGenerator(ll: Point, ur: Point, minLength: Double, maxLength: Double, crs: CrsId = CrsId.UNDEFINED) {

  def this(minX: Double, minY: Double, maxX: Double, maxY: Double, minLength: Double, maxLength: Double, epsg: Int = 31370) = {
    this(Points.create2D(minX, minY, CrsId.valueOf(epsg)), Points.create2D(maxX, maxY, CrsId.valueOf(epsg)), minLength, maxLength, CrsId.valueOf(epsg))
  }

  require(ll.getX < ur.getX && ll.getY < ur.getY && minLength < maxLength)

  val xRange = ur.getX - ll.getX
  val yRange = ur.getY - ll.getY

  def pointCoordinates: (Double, Double) = (ll.getX + Random.nextDouble() * xRange, ll.getY + Random.nextDouble() * yRange)

  def length = minLength + Random.nextDouble() * (maxLength - minLength)

  /**
   * Generate an angle in the range of [-PI/2, PI/2)
   * @return
   */
  def angle = -Math.PI / 2 + Random.nextDouble() * 2 * Math.PI / 2

  def numCoordinates = 2 + Random.nextInt(30)

  def line: LineString = {
    val n = numCoordinates
    val psBuilder = PointSequenceBuilders.fixedSized(n, DimensionalFlag.d2D, crs)
    var i = 0
    var cx = pointCoordinates
    while (i < n) {
      psBuilder.add(cx._1, cx._2)
      val a = angle
      val l = length
      cx = (cx._1 + l * Math.cos(a), cx._2 + l * Math.sin(a))
      i += 1
    }
    new LineString(psBuilder.toPointSequence)
  }

  def feature(id: Int): JsObject = {
    Json.obj(
      "geometry" -> Json.toJson(line),
      "id" -> id,
      "properties" -> Json.obj("foo" -> "bar")
    )
  }

  def generateToCollection(num: Int, dbName: String, colName: String, level: Int = 8) {

    val it = new Iterator[JsObject] {
      var cnt = 0

      def hasNext: Boolean = cnt < num

      def next(): JsObject = {
        val f = feature(cnt)
        cnt += 1
        f
      }
    }

    //TODO -- fix this data generator (requires completing FeatureFormat)

//    import ExecutionContext.Implicits.global
//    val sink = new MongoDbSink(dbName, colName)
//    sink.in(Enumerator.enumerate(it))

  }

}




