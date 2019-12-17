import java.io.{BufferedWriter, File, FileWriter}

import com.google.gson.JsonObject
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("TrafficHeatmap")
    .config("spark.master", "local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  val dbManager: DBmanager = new DBmanager(spark)
  val tableName: String = "beijing_taxi"

  val allDataDF: DataFrame = dbManager.loadTable(tableName)

  allDataDF.createOrReplaceTempView("allDataDF_view")
  val taxiPositions:DataFrame = spark.sql("SELECT ROUND(lng,2) as agg_lng, ROUND(lat,2) as agg_lat, vehicleId, timestamp FROM allDataDF_view WHERE lng >= 116.1 AND lng <= 116.7 AND lat >= 39.7 AND lat <= 40.2")

  taxiPositions.createOrReplaceTempView("taxiPositions_view")
  val heatmapRectangles:DataFrame = spark.sql("SELECT agg_lng, agg_lat, count(*) as count FROM taxiPositions_view GROUP BY agg_lng, agg_lat ORDER BY agg_lng, agg_lat")

  val geoJsonTools: GeoJsonTools = new GeoJsonTools()
  val gsonTrajectory: JsonObject = geoJsonTools.convertToRectanglesHeatmap(heatmapRectangles)

  val file = new File("rectangles_heatmap.json")
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(gsonTrajectory.toString)
  bw.close()
}
