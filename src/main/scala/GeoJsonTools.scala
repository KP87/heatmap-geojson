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

  private def generateFeatures(rectanglesDF:DataFrame): JsonArray = {
    val minMax: Row = rectanglesDF.agg(min("count"), max("count")).head()
    val valueRanges: Seq[Int] = rangeArray(minMax.getLong(0), minMax.getLong(1), 4)

    var features = new JsonArray()
    rectanglesDF.collect.foreach(item => {
      val coordinates = new Coordinates(round(item.getDouble(0), 2), round(item.getDouble(1), 2))
      val value: Int = item.getLong(2).toInt
      val color: String = setColor(value, valueRanges)

      features.add(generatePolygon(coordinates, color, value))
    })
    features
  }

  private def generatePolygon(coordinates: Coordinates, color:String, value:Int): JsonObject = {
    def generateGeometry(coordinates: Coordinates): JsonObject = {
      val lng: Double = coordinates.lng
      val lat: Double = coordinates.lat

      val borderPoints = new JsonArray()

      val NWpoint = new JsonArray()
      NWpoint.add(lng - 0.005)
      NWpoint.add(lat + 0.005)

      val NEpoint = new JsonArray()
      NEpoint.add(lng + 0.005)
      NEpoint.add(lat + 0.005)

      val SEpoint = new JsonArray()
      SEpoint.add(lng + 0.005)
      SEpoint.add(lat - 0.005)

      val SWpoint = new JsonArray()
      SWpoint.add(lng - 0.005)
      SWpoint.add(lat - 0.005)

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

  private def setColor(value:Int, valueLevels:Seq[Int]): String = {
    def toRGB(h:Float, s:Float, l:Float, alpha:Float):Color = {
      if (s <0.0f || s > 100.0f)
      {
        val message = "Color parameter outside of expected range - Saturation";
        throw new IllegalArgumentException( message );
      }

      if (l <0.0f || l > 100.0f)
      {
        val message = "Color parameter outside of expected range - Luminance";
        throw new IllegalArgumentException( message );
      }

      if (alpha <0.0f || alpha > 1.0f)
      {
        val message = "Color parameter outside of expected range - Alpha";
        throw new IllegalArgumentException( message );
      }

      //  Formula needs all values between 0 - 1.
      val h2: Float = h % 360.0f
      val h3: Float = h2/360f
      val s2: Float = s/100f
      val l2: Float = l/100f

      //    println(s"l2 = $l2, s2=$s2")
      var q:Float = if (l2 < 0.5) l2 * (1 + s2) else (l2 + s2) - (s2 * l2)
      var p:Float = 2 * l2 - q

      def HueToRGB(p:Float, q:Float, h:Float):Float = {
        val h2 = if (h < 0) h + 1 else if (h > 1) h - 1 else h

        if (6.0f * h2 < 1) {
          p + ((q - p) * 6.0f * h2)
        } else if (2.0f * h2 < 1){
          q
        } else if (3.0f * h2 < 2) {
          p + ( (q - p) * 6.0f * ((2.0f / 3.0f) - h2) )
        } else {
          p
        }
      }

      var r:Float = Math.max(0, HueToRGB(p, q, h3 + (1.0f / 3.0f)));
      var g:Float = Math.max(0, HueToRGB(p, q, h3));
      var b:Float = Math.max(0, HueToRGB(p, q, h3 - (1.0f / 3.0f)));
      r = Math.min(r, 1.0f);
      g = Math.min(g, 1.0f);
      b = Math.min(b, 1.0f);

      return new Color(r, g, b, alpha);
    }

    val level = (1-(value-valueLevels(0)).toDouble/(valueLevels(4)-valueLevels(0)).toDouble)*180
    val color = toRGB(level.toFloat, 100, 50, 1)

    "rgba(" + color.getRed + ", " + color.getGreen + ", " + color.getBlue + ", 1)"
  }
}
