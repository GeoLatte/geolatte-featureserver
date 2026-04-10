package persistence

import org.geolatte.geom._
import org.geolatte.geom.crs.{
  CoordinateReferenceSystem,
  CoordinateReferenceSystems,
  CrsId,
  CrsRegistry
}
import play.api.libs.functional.syntax._
import play.api.libs.json.{ Json, _ }

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/18/13
 *
 * Migrated to geolatte-geom 1.11. All public types use wildcard `[_]` parameters
 * because the position type is determined at runtime from the CRS lookup +
 * coordinate-array dimensionality. The `withCapturedCrs` helper opens the
 * wildcard into a fresh type variable `P` via Scala's standard wildcard-capture
 * mechanism. Two narrow post-capture casts (`Envelope[_]→Envelope[P]` in
 * `toPolygon`, `Geometry[_]→Geometry[P]` for `GeometryCollection` children)
 * are used where the Scala type system cannot express the structural invariant
 * statically.
 */
object GeoJsonFormats {

  // ---------------------------------------------------------------------------
  // Wildcard capture helper
  // ---------------------------------------------------------------------------

  /**
   * Single-method-polymorphic SAM. Cannot be a Scala function because Scala 2.x
   * function types are monomorphic; this trait provides the rank-2 polymorphism
   * we need to capture an existential CRS type into a fresh type variable.
   */
  trait CrsHandler[R] {
    def apply[P <: Position](crs: CoordinateReferenceSystem[P]): R
  }

  /**
   * Open the existential CRS[_] into a fresh `P` and pass it to the handler.
   * Scala 2.13 performs wildcard capture automatically when a wildcard is passed
   * to a parametrically-polymorphic method parameter — there are no casts here.
   */
  def withCapturedCrs[R](crs: CoordinateReferenceSystem[_])(h: CrsHandler[R]): R =
    h.apply(crs)

  // ---------------------------------------------------------------------------
  // CRS resolution + dimensional promotion
  // ---------------------------------------------------------------------------

  /** Default base CRS used when an EPSG code is not found in the registry. */
  private val DefaultBaseCrs: CoordinateReferenceSystem[_] =
    CoordinateReferenceSystems.PROJECTED_2D_METER

  /** Look up a (2D base) CRS by EPSG code, falling back to DefaultBaseCrs. */
  def lookupCrs(epsg: Int): CoordinateReferenceSystem[_] =
    CrsRegistry.getCoordinateReferenceSystemForEPSG(epsg, DefaultBaseCrs)

  /**
   * Promote a 2D base CRS to its 2D / 3D / 2DM / 3DM variant matching the
   * coordinate count from the JSON. Old code distinguished `2DM` from `3D` via
   * `coordinates(2).isNaN`, so we preserve that convention.
   *
   * Note: `CoordinateReferenceSystems.adjustTo` is a Java method; Scala does not
   * support named parameters for Java methods, so we use positional arguments
   * (first boolean = hasZ, second boolean = hasM).
   */
  def adjustCrsToCoords(
      base:      CoordinateReferenceSystem[_],
      numCoords: Int,
      coords:    Array[Double]
  ): CoordinateReferenceSystem[_] = numCoords match {
    case 2 => base
    case 3 => CoordinateReferenceSystems.adjustTo(base, true, false)
    case 4 if coords(2).isNaN =>
      CoordinateReferenceSystems.adjustTo(base, false, true)
    case 4 =>
      CoordinateReferenceSystems.adjustTo(base, true, true)
    case _ =>
      throw new IllegalArgumentException(s"Unexpected coordinate count: $numCoords")
  }

  // ---------------------------------------------------------------------------
  // Envelope format
  // ---------------------------------------------------------------------------

  implicit val EnvelopeFormats: Format[Envelope[_]] = new Format[Envelope[_]] {

    def reads(json: JsValue): JsResult[Envelope[_]] =
      Try {
        val extent = (json \ "envelope").get
        val crs    = (json \ "crs").as[Int]
        toEnvelope(extent, lookupCrs(crs))
      }.recover {
        case t: Throwable => JsError(JsonValidationError(t.getMessage))
      }.get

    def writes(e: Envelope[_]): JsValue = Json.obj(
      "crs" -> e.getCoordinateReferenceSystem.getCrsId.getCode,
      "envelope" -> (if (e.isEmpty) {
        JsArray()
      } else {
        val a = e.toArray() // [xmin, ymin, xmax, ymax]
        Json.arr(a(0), a(1), a(2), a(3))
      })
    )

    def toEnvelope(jsValue: JsValue, crs: CoordinateReferenceSystem[_]): JsResult[Envelope[_]] =
      jsValue match {
        case array: JsArray if array.value.isEmpty =>
          JsSuccess(emptyEnvelope(crs))
        case array: JsArray =>
          Try {
            val xmin = array.head.as[Double]
            val ymin = array.value(1).as[Double]
            val xmax = array.value(2).as[Double]
            val ymax = array.value(3).as[Double]
            JsSuccess(buildEnvelope(crs, xmin, ymin, xmax, ymax))
          }.getOrElse(JsError(JsonValidationError(s"Array $array can't be turned into a valid boundingbox")))
        case _ => JsError("Json value is not an array")
      }
  }

  /** Empty envelope on the given (wildcard) CRS, captured to bind P. */
  private def emptyEnvelope(crs: CoordinateReferenceSystem[_]): Envelope[_] =
    withCapturedCrs(crs)(new CrsHandler[Envelope[_]] {
      def apply[P <: Position](c: CoordinateReferenceSystem[P]): Envelope[_] = new Envelope(c)
    })

  /** Build a 2D envelope on the given (wildcard) CRS. */
  def buildEnvelope(
      crs: CoordinateReferenceSystem[_],
      xmin: Double, ymin: Double, xmax: Double, ymax: Double
  ): Envelope[_] =
    withCapturedCrs(crs)(new CrsHandler[Envelope[_]] {
      def apply[P <: Position](c: CoordinateReferenceSystem[P]): Envelope[_] =
        new Envelope[P](xmin, ymin, xmax, ymax, c)
    })

  // ---------------------------------------------------------------------------
  // CrsId reads/writes (for the GeoJSON "crs" property)
  // ---------------------------------------------------------------------------

  implicit val crsWrites: Writes[CrsId] = (
    (__ \ "type").write[String] and
    (__ \ "properties" \ "name").write[String]
  )((to: CrsId) => ("name", to.toString))

  implicit val crsReads: Reads[CrsId] = (
    (__ \ "type").read[String] and
    (__ \ "properties" \ "name").read[String]
  )((_: String, name: String) => CrsId.parse(name))

  // ---------------------------------------------------------------------------
  // Geometry reads
  // ---------------------------------------------------------------------------

  /**
   * Build a PositionSequence[P] from an Array[Array[Double]] of coordinates.
   * The CRS must already be dim-promoted to match `coordinates(0).length`.
   */
  private def buildPositionSequence[P <: Position](
      crs:         CoordinateReferenceSystem[P],
      coordinates: Array[Array[Double]]
  ): PositionSequence[P] = {
    val builder = PositionSequenceBuilders.variableSized(crs.getPositionClass)
    coordinates.foreach { coord =>
      builder.add(Positions.mkPosition(crs, coord: _*))
    }
    builder.toPositionSequence
  }

  /**
   * Build a single Point[P] from a coordinate array. CRS must be dim-promoted.
   */
  private def buildTypedPoint[P <: Position](
      crs:    CoordinateReferenceSystem[P],
      coords: Array[Double]
  ): Point[P] = Geometries.mkPoint(Positions.mkPosition(crs, coords: _*), crs)

  /**
   * Top-level geometry reads. Reads the "crs" field if present (else uses
   * `defaultEpsg`), looks up the base CRS, then dispatches per "type".
   */
  def mkGeometryReads(defaultEpsg: Int): Reads[Geometry[_]] =
    Reads { js =>
      Try {
        val typeDiscriminator = (js \ "type").as[String]
        val crsOpt            = (js \ "crs").asOpt[CrsId]
        val baseCrs           = crsOpt match {
          case Some(c) => CrsRegistry.getCoordinateReferenceSystemForEPSG(c.getCode, DefaultBaseCrs)
          case None    => CrsRegistry.getCoordinateReferenceSystemForEPSG(defaultEpsg, DefaultBaseCrs)
        }
        readGeometry(typeDiscriminator, baseCrs, js)
      }.fold(
        t => JsError(JsonValidationError(t.getMessage)),
        g => JsSuccess(g)
      )
    }

  /** Default geometryReads uses CrsId.UNDEFINED — i.e. no CRS in JSON. */
  implicit val geometryReads: Reads[Geometry[_]] = mkGeometryReads(CrsId.UNDEFINED.getCode)

  /** Pick from "geometry" sub-object then read. */
  val geoJsonGeometryReads: Reads[Geometry[_]] =
    (__ \ "geometry").json.pick[JsObject] andThen geometryReads

  /**
   * Promote `baseCrs` to match the dimensionality of `sampleCoord`, then
   * capture the resulting wildcard CRS into a fresh `P` for the `build`
   * handler. Extracted to deduplicate the per-geometry-type branches in
   * `readGeometry`.
   */
  private def readTyped(
      baseCrs:     CoordinateReferenceSystem[_],
      sampleCoord: Array[Double]
  )(build: CrsHandler[Geometry[_]]): Geometry[_] = {
    val crs = adjustCrsToCoords(baseCrs, sampleCoord.length, sampleCoord)
    withCapturedCrs(crs)(build)
  }

  /**
   * Extract the first coordinate array from a GeoJSON geometry JsValue.
   * Used to determine the dimensionality for a GeometryCollection before
   * parsing its children.
   */
  private def firstCoordinateOf(js: JsValue): Array[Double] = {
    // For GeometryCollection children, recurse into their own first child.
    val tpe = (js \ "type").as[String]
    if (tpe == "GeometryCollection") {
      val nested = (js \ "geometries").as[Array[JsValue]]
      if (nested.isEmpty) Array(0.0, 0.0) else firstCoordinateOf(nested(0))
    } else {
      // coordinates is either Array[Double] (Point) or nested arrays (others).
      // Drill down until we reach the innermost Double array.
      var cur: JsValue = (js \ "coordinates").get
      while (cur.isInstanceOf[JsArray] && cur.asInstanceOf[JsArray].value.headOption.exists(_.isInstanceOf[JsArray])) {
        cur = cur.asInstanceOf[JsArray].value.head
      }
      cur.as[Array[Double]]
    }
  }

  /**
   * Parse a GeoJSON geometry with a pre-resolved, typed CRS. All children
   * share the caller's `P` by construction, so no `asInstanceOf` cast is
   * needed. Used inside GeometryCollection parsing.
   */
  private def readGeometryTyped[P <: Position](
      typeDiscriminator: String,
      crs:               CoordinateReferenceSystem[P],
      js:                JsValue
  ): Geometry[P] = typeDiscriminator match {
    case "Point" =>
      val coords = (js \ "coordinates").as[Array[Double]]
      buildTypedPoint(crs, coords)
    case "LineString" =>
      val coords = (js \ "coordinates").as[Array[Array[Double]]]
      Geometries.mkLineString(buildPositionSequence(crs, coords), crs)
    case "Polygon" =>
      val rings = (js \ "coordinates").as[Array[Array[Array[Double]]]]
      val typedRings = rings.map(r => Geometries.mkLinearRing(buildPositionSequence(crs, r), crs))
      Geometries.mkPolygon(typedRings: _*)
    case "MultiPoint" =>
      val coords = (js \ "coordinates").as[Array[Array[Double]]]
      val pts = coords.map(co => buildTypedPoint(crs, co))
      Geometries.mkMultiPoint(pts: _*)
    case "MultiLineString" =>
      val lines = (js \ "coordinates").as[Array[Array[Array[Double]]]]
      val typedLines = lines.map(l => Geometries.mkLineString(buildPositionSequence(crs, l), crs))
      Geometries.mkMultiLineString(typedLines: _*)
    case "MultiPolygon" =>
      val polys = (js \ "coordinates").as[Array[Array[Array[Array[Double]]]]]
      val typedPolys = polys.map { rings =>
        val typedRings = rings.map(r => Geometries.mkLinearRing(buildPositionSequence(crs, r), crs))
        Geometries.mkPolygon(typedRings: _*)
      }
      Geometries.mkMultiPolygon(typedPolys: _*)
    case "GeometryCollection" =>
      val nested = (js \ "geometries").as[Array[JsValue]]
      val typedChildren = nested.map(g => readGeometryTyped[P]((g \ "type").as[String], crs, g))
      Geometries.mkGeometryCollection(typedChildren: _*)
    case other =>
      throw new IllegalArgumentException(s"Unknown geometry type: $other")
  }

  /**
   * Dispatch on geometry type. Each branch reads the coordinates, extracts a
   * sample coordinate for dimensional promotion, and delegates to `readTyped`
   * which promotes the base CRS and opens the wildcard into `[P]`.
   */
  private def readGeometry(
      typeDiscriminator: String,
      baseCrs:           CoordinateReferenceSystem[_],
      js:                JsValue
  ): Geometry[_] = typeDiscriminator match {

    case "Point" =>
      val coords = (js \ "coordinates").as[Array[Double]]
      readTyped(baseCrs, coords)(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] =
          buildTypedPoint(c, coords)
      })

    case "LineString" =>
      val coords = (js \ "coordinates").as[Array[Array[Double]]]
      readTyped(baseCrs, coords(0))(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] =
          Geometries.mkLineString(buildPositionSequence(c, coords), c)
      })

    case "Polygon" =>
      val rings = (js \ "coordinates").as[Array[Array[Array[Double]]]]
      readTyped(baseCrs, rings(0)(0))(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] = {
          val typedRings = rings.map(r => Geometries.mkLinearRing(buildPositionSequence(c, r), c))
          Geometries.mkPolygon(typedRings: _*)
        }
      })

    case "MultiPoint" =>
      val coords = (js \ "coordinates").as[Array[Array[Double]]]
      readTyped(baseCrs, coords(0))(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] = {
          val pts = coords.map(co => buildTypedPoint(c, co))
          Geometries.mkMultiPoint(pts: _*)
        }
      })

    case "MultiLineString" =>
      val lines = (js \ "coordinates").as[Array[Array[Array[Double]]]]
      readTyped(baseCrs, lines(0)(0))(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] = {
          val typedLines = lines.map(l => Geometries.mkLineString(buildPositionSequence(c, l), c))
          Geometries.mkMultiLineString(typedLines: _*)
        }
      })

    case "MultiPolygon" =>
      val polys = (js \ "coordinates").as[Array[Array[Array[Array[Double]]]]]
      readTyped(baseCrs, polys(0)(0)(0))(new CrsHandler[Geometry[_]] {
        def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] = {
          val typedPolys = polys.map { rings =>
            val typedRings = rings.map(r => Geometries.mkLinearRing(buildPositionSequence(c, r), c))
            Geometries.mkPolygon(typedRings: _*)
          }
          Geometries.mkMultiPolygon(typedPolys: _*)
        }
      })

    case "GeometryCollection" =>
      val geometries = (js \ "geometries").as[Array[JsValue]]
      if (geometries.isEmpty) {
        withCapturedCrs(baseCrs)(new CrsHandler[Geometry[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] =
            Geometries.mkEmptyGeometryCollection(c)
        })
      } else {
        // Determine the collection's CRS from the first child's coordinates,
        // then parse ALL children within the same typed [P] scope. This
        // guarantees type-level homogeneity by construction — no asInstanceOf
        // cast needed, and no post-hoc CRS validation.
        //
        // Children with fewer coordinates than P expects will have their
        // missing ordinates defaulted (z=0, m=NaN) by Positions.mkPosition,
        // matching pre-migration behaviour where all children shared a single
        // unparameterized CrsId.
        val firstCoords = firstCoordinateOf(geometries(0))
        readTyped(baseCrs, firstCoords)(new CrsHandler[Geometry[_]] {
          def apply[P <: Position](c: CoordinateReferenceSystem[P]): Geometry[_] = {
            val typedChildren = geometries.map(g =>
              readGeometryTyped[P]((g \ "type").as[String], c, g)
            )
            Geometries.mkGeometryCollection(typedChildren: _*)
          }
        })
      }

    case other =>
      throw new IllegalArgumentException(s"Unknown geometry type: $other")
  }

  // ---------------------------------------------------------------------------
  // Feature validator
  // ---------------------------------------------------------------------------

  def featureValidator(idType: String): Reads[JsObject] = idType match {
    case "decimal" => (__ \ "id").read[Long] andKeep __.read[JsObject]
    case "text"    => (__ \ "id").read[String] andKeep __.read[JsObject]
    case _         => throw new IllegalArgumentException("Invalid metadata")
  }

  // ---------------------------------------------------------------------------
  // Geometry writes
  // ---------------------------------------------------------------------------

  // alle GeoJsonTo classes delen deze properties
  private val baseGeoJsonToWrites = {
    (__ \ "type").write[String] and
      (__ \ "crs").write[CrsId] and
      (__ \ "bbox").write[Array[Double]]
  }

  /**
   * Extract coordinate array from a PositionSequence without touching the
   * P-typed position values — uses index-based `getCoordinates` instead.
   *
   * Wire-format rule for 2DM positions: emit [x, y, 0, m] (4 elements with
   * z=0), matching the original pre-migration behaviour. `getCoordinateDimension`
   * returns 3 for C2DM (x, y, m) but consumers expect a 4-element array.
   */
  private def is2DM(ps: PositionSequence[_]): Boolean = {
    // PositionSequence does not expose its CRS. The position class name
    // (`C2DM`/`G2DM`) is the simplest cast-free way to detect the 2DM family.
    val name = ps.getPositionClass.getSimpleName
    name == "C2DM" || name == "G2DM"
  }

  private def positionToArray(ps: PositionSequence[_], i: Int): Array[Double] = {
    val dim = ps.getCoordinateDimension
    val buf = new Array[Double](dim)
    ps.getCoordinates(i, buf)
    if (is2DM(ps)) {
      // buf = [x, y, m]; expand to [x, y, 0, m]
      Array(buf(0), buf(1), 0.0, buf(2))
    } else {
      buf
    }
  }

  private def seqCoordinates(ps: PositionSequence[_]): Array[Array[Double]] =
    (0 until ps.size).map(i => positionToArray(ps, i)).toArray

  private def coordinates(p: Point[_]): Array[Double] = {
    val ps = p.getPositions
    positionToArray(ps, 0)
  }

  private def coordinates(l: LineString[_]): Array[Array[Double]] =
    seqCoordinates(l.getPositions)

  private def coordinates(p: Polygon[_]): Array[Array[Array[Double]]] = {
    val ext   = coordinates(p.getExteriorRing)
    val holes = (0 until p.getNumInteriorRing).map(i => coordinates(p.getInteriorRingN(i)))
    (Seq(ext) ++ holes).toArray
  }

  private def coordinates(m: MultiPoint[_]): Array[Array[Double]] =
    (0 until m.getNumGeometries).map(i => coordinates(m.getGeometryN(i))).toArray

  private def coordinates(m: MultiLineString[_]): Array[Array[Array[Double]]] =
    (0 until m.getNumGeometries).map(i => coordinates(m.getGeometryN(i))).toArray

  private def coordinates(m: MultiPolygon[_]): Array[Array[Array[Array[Double]]]] =
    (0 until m.getNumGeometries).map(i => coordinates(m.getGeometryN(i))).toArray

  private def bbox(input: Geometry[_]): Array[Double] = {
    if (input == null || input.isEmpty) {
      Array()
    } else {
      // Envelope.toArray() returns [xmin, ymin, xmax, ymax] without exposing P-typed positions
      input.getEnvelope.toArray()
    }
  }

  private def crsId(g: Geometry[_]): CrsId = g.getCoordinateReferenceSystem.getCrsId

  implicit val pointWrites: Writes[Point[_]] = new Writes[Point[_]] {
    override def writes(o: Point[_]): JsValue =
      if (o.isEmpty) {
        JsNull
      } else {
        (
          baseGeoJsonToWrites and
          (__ \ "coordinates").write[Array[Double]]
        )((p: Point[_]) => ("Point", crsId(p), bbox(p), coordinates(p))).writes(o)
      }
  }

  implicit val linestringWrites: Writes[LineString[_]] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Double]]]
  )((l: LineString[_]) => ("LineString", crsId(l), bbox(l), coordinates(l)))

  implicit val polygonWrites: Writes[Polygon[_]] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Double]]]]
  )((l: Polygon[_]) => ("Polygon", crsId(l), bbox(l), coordinates(l)))

  implicit val multilinestringWrites: Writes[MultiLineString[_]] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Double]]]]
  )((m: MultiLineString[_]) => ("MultiLineString", crsId(m), bbox(m), coordinates(m)))

  implicit val multipolygonWrites: Writes[MultiPolygon[_]] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Array[Array[Double]]]]]
  )((m: MultiPolygon[_]) => ("MultiPolygon", crsId(m), bbox(m), coordinates(m)))

  implicit val multipointWrites: Writes[MultiPoint[_]] = (
    baseGeoJsonToWrites and
    (__ \ "coordinates").write[Array[Array[Double]]]
  )((m: MultiPoint[_]) => ("MultiPoint", crsId(m), bbox(m), coordinates(m)))

  implicit val geometryWrites: Writes[Geometry[_]] = new Writes[Geometry[_]] {
    def writes(g: Geometry[_]): JsValue = g match {
      case x: Point[_]              => pointWrites.writes(x)
      case x: LineString[_]         => linestringWrites.writes(x)
      case x: Polygon[_]            => polygonWrites.writes(x)
      case x: MultiPoint[_]         => multipointWrites.writes(x)
      case x: MultiLineString[_]    => multilinestringWrites.writes(x)
      case x: MultiPolygon[_]       => multipolygonWrites.writes(x)
      case x: GeometryCollection[_] => geometryCollectionWrites.writes(x)
    }
  }

  implicit lazy val geometryCollectionWrites: Writes[GeometryCollection[_]] = (
    baseGeoJsonToWrites and
    (__ \ "geometries").write[Array[JsValue]]
  )((g: GeometryCollection[_]) => {
    // Avoid the iterator-as-Scala existential leak by indexing directly.
    val childJsons: Array[JsValue] =
      (0 until g.getNumGeometries).map(i => geometryWrites.writes(g.getGeometryN(i))).toArray
    ("GeometryCollection", crsId(g), bbox(g), childJsons)
  })

  // ---------------------------------------------------------------------------
  // Envelope → polygon utility
  // ---------------------------------------------------------------------------

  /** Build a 5-point closed polygon from an envelope. */
  def toPolygon(envelope: Envelope[_]): Polygon[_] =
    withCapturedCrs(envelope.getCoordinateReferenceSystem)(new CrsHandler[Polygon[_]] {
      def apply[P <: Position](c: CoordinateReferenceSystem[P]): Polygon[_] = {
        // Envelope.toArray() works on Envelope[_] — no narrowing cast needed.
        val a = envelope.toArray() // [xmin, ymin, xmax, ymax]
        val (xMin, yMin, xMax, yMax) = (a(0), a(1), a(2), a(3))
        val builder = PositionSequenceBuilders.fixedSized(5, c.getPositionClass)
        builder.add(Positions.mkPosition(c, xMin, yMin))
        builder.add(Positions.mkPosition(c, xMax, yMin))
        builder.add(Positions.mkPosition(c, xMax, yMax))
        builder.add(Positions.mkPosition(c, xMin, yMax))
        builder.add(Positions.mkPosition(c, xMin, yMin))
        val ring = Geometries.mkLinearRing(builder.toPositionSequence, c)
        Geometries.mkPolygon(ring)
      }
    })
}
