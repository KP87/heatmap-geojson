import java.awt.Color

import com.google.gson.{JsonArray, JsonObject}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, Row}

import scala.math.BigDecimal.RoundingMode

class GeoJsonTools extends Serializable {
  def convertToRectanglesHeatmap(rectanglesDF:DataFrame): JsonObject = {
    val gsonTrajectory = new JsonObject
    gsonTrajectory.addProperty("type", "FeatureCollection")
    gsonTrajectory.add("features", generateFeatures(rectanglesDF:DataFrame))

    gsonTrajectory
  }

  def generateFeatures(rectanglesDF:DataFrame): JsonArray = {
    val minMax: Row = rectanglesDF.agg(min("count"), max("count")).head()
    val valueRanges: Seq[Int] = rangeArray(minMax.getLong(0), minMax.getLong(1), 4)

    var features = new JsonArray()
    // TODO: find another JSON library for work with serialization
    rectanglesDF.collect.foreach(item => {
      val coordinates: Coordinates = new Coordinates(round(item.getDouble(0), 2), round(item.getDouble(1), 2))
      val value: Int = item.getLong(2).toInt
      val color: String = setColor(value, valueRanges)

      features.add(generatePolygon(coordinates, color, value))
    })
    features
  }

  def generatePolygon(coordinates: Coordinates, color:String, value:Int): JsonObject = {
    def generateGeometry(coordinates: Coordinates): JsonObject = {
      val borderPoints = new JsonArray()

      val NWpoint = new JsonArray()
      NWpoint.add(coordinates.lng - 0.005)
      NWpoint.add(coordinates.lat + 0.005)

      val NEpoint = new JsonArray()
      NEpoint.add(coordinates.lng + 0.005)
      NEpoint.add(coordinates.lat + 0.005)

      val SEpoint = new JsonArray()
      SEpoint.add(coordinates.lng + 0.005)
      SEpoint.add(coordinates.lat - 0.005)

      val SWpoint = new JsonArray()
      SWpoint.add(coordinates.lng - 0.005)
      SWpoint.add(coordinates.lat - 0.005)

      val points = new JsonArray()
      points.add(NWpoint)
      points.add(NEpoint)
      points.add(SEpoint)
      points.add(SWpoint)
      points.add(NWpoint)

      borderPoints.add(points)

      val geometry = new JsonObject
      geometry.addProperty("type", "Polygon")
      geometry.add("coordinates", borderPoints)

      geometry
    }

    val properties = new JsonObject
    properties.addProperty("value", value)
    properties.addProperty("fill", color)
    properties.addProperty("fill-opacity", 0.5)
    properties.addProperty("stroke-width", 0)

    val feature = new JsonObject
    feature.addProperty("type", "Feature")
    feature.add("geometry", generateGeometry(coordinates))
    feature.add("properties", properties)
    feature
  }

  private def round(value: Double, places: Int): Double ={
    BigDecimal(value.toString).setScale(places, RoundingMode.HALF_UP).toDouble
  }

  private def rangeArray(minVal:Long, maxVal:Long, numberOfIntervals: Int): Seq[Int] = {
    for (k <- 0 to numberOfIntervals) yield {
      (minVal + Math.round(k.toDouble/numberOfIntervals.toDouble*(maxVal-minVal))).toInt
    }
  }

  def setColor(value:Int, valueLevels:Seq[Int]): String = {
    val level: Double = 1 - (value - valueLevels.head).toDouble/(valueLevels.last - valueLevels.head).toDouble
    val hue: Float = level.toFloat * 180.0f

    val color: Color = new ColorConverter().HSLAtoRGBA(hue, 100.0f, 50.0f, 100.0f)

    s"rgba(${color.getRed}, ${color.getGreen}, ${color.getBlue}, ${color.getAlpha})"
  }
}
