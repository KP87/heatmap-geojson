import java.io.{BufferedWriter, File, FileWriter}
import java.util.Properties

import com.google.gson.JsonObject
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("TrafficHeatmap")
    .config("spark.master", "local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  val geoJsonTools: GeoJsonTools = new GeoJsonTools()

  def main(args: Array[String]): Unit = {
    val url = "jdbc:postgresql://127.0.0.1:5432/test_dataset"
    val tableName = "beijing_taxi"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val allDataDF: DataFrame = spark.read.jdbc(url, tableName, connectionProperties)

    val parameters = HeatmapParameters(116.1,116.7,39.7,40.2,2)
    val trafficVolume: DataFrame = calculateTrafficVolume(allDataDF, parameters)

    val trafficVolumeGeoJSON: JsonObject = runWithTimeOfExecution(geoJsonTools.convertToRectanglesHeatmap(trafficVolume))

    saveToFile(trafficVolumeGeoJSON, "rectangles_heatmap.json")
  }

  def calculateTrafficVolume(allDataDF: DataFrame, parameters: HeatmapParameters): DataFrame = {
    import org.apache.spark.sql.functions.{col, count, round}

    allDataDF
      .filter(col("lng").between(parameters.westBound, parameters.eastBound))
      .filter(col("lat").between(parameters.southBound, parameters.northBound))
      .groupBy(
        round(col("lng"), parameters.discretization).as("agg_lng"),
        round(col("lat"), parameters.discretization).as("agg_lat")
      )
      .agg(count("*").as("count"))
  }

  def calculateTrafficVolumeSQL(allDataDF: DataFrame, parameters: HeatmapParameters): DataFrame = {
    val firstViewName = "allDataDF_view"
    allDataDF.createOrReplaceTempView(firstViewName)
    val filteredTaxiPositions: DataFrame = spark.sql(
      s"SELECT " +
        s"ROUND(lng,${parameters.discretization}) as agg_lng, " +
        s"ROUND(lat,${parameters.discretization}) as agg_lat " +
      s"FROM " +
        s"$firstViewName " +
      s"WHERE " +
        s"lng BETWEEN ${parameters.westBound} AND ${parameters.eastBound} AND " +
        s"lat BETWEEN ${parameters.southBound} AND ${parameters.northBound}"
    )

    val secondViewName = "taxiPositions_view"
    filteredTaxiPositions.createOrReplaceTempView(secondViewName)
    val aggregatedTaxiPositions: DataFrame = spark.sql(
      s"SELECT agg_lng, agg_lat, count(*) as count " +
        s"FROM $secondViewName " +
        s"GROUP BY agg_lng, agg_lat"
    )

    return aggregatedTaxiPositions
  }

  private def saveToFile(gsonTrajectory: JsonObject, fileName: String): Unit = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(gsonTrajectory.toString)
    bw.close()
  }

  def runWithTimeOfExecution[T](anyFunction: => T): T = {
    val startTime: Double = System.nanoTime().toDouble
    val output: T = anyFunction
    println(s"Executed in:\t${(System.nanoTime().toDouble - startTime)/1000000000} s")

    output
  }
}
