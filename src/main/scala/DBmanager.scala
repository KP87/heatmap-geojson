import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class DBmanager(spark:SparkSession) {
  private val url = "jdbc:postgresql://127.0.0.1:5432/test_dataset"
  private val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("driver", "org.postgresql.Driver")

  def loadTable(tableName:String): DataFrame = {
    spark.read.jdbc(url, tableName, connectionProperties)
  }
}
