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
}
