import java.awt.Color

import com.google.gson.JsonObject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit._

class ApplicationSuite {
  @BeforeClass
  val spark: SparkSession = SparkSession
      .builder()
      .appName("TrafficHeatmap")
      .config("spark.master", "local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  @BeforeClass
  // To prepare test data, saved  in "testDF" path, execute following code in Application.main method:
  // allDataDF.sample(100.0d/allDataDF.count()).write.parquet("testDF")
  val testDF: DataFrame  = spark.read.parquet("testDF")
  @BeforeClass
  val colorConverter: ColorConverter = new ColorConverter()
  @BeforeClass
  val geoJsonTools: GeoJsonTools  = new GeoJsonTools()
  @BeforeClass
  val parameters = HeatmapParameters(116.1,116.7,39.7,40.2,2)

  @Test(expected = classOf[IllegalArgumentException]) def shouldThrownIllegalArgumentExceptionOnWrongHueValue(): Unit = {
    colorConverter.HSLAtoRGBA(-180, 100, 100, 100)
  }

  @Test(expected = classOf[IllegalArgumentException]) def shouldThrownIllegalArgumentExceptionOnWrongSaturationValue(): Unit = {
    colorConverter.HSLAtoRGBA(180, -100, 100, 100)
  }

  @Test(expected = classOf[IllegalArgumentException]) def shouldThrownIllegalArgumentExceptionOnWrongLuminanceValue(): Unit = {
    colorConverter.HSLAtoRGBA(180, 100, -100, 100)
  }

  @Test(expected = classOf[IllegalArgumentException]) def shouldThrownIllegalArgumentExceptionOnWrongAlphaValue(): Unit = {
    colorConverter.HSLAtoRGBA(180, 100, 100, -100)
  }

  @Test def shouldConvertFromHSLAtoRGBA() = {
    val expected = new Color(0.49999988f, 1, 0, 1)
    val actual = colorConverter.HSLAtoRGBA(90.0f, 100.0f, 50.0f, 100.0f)
    Assert.assertEquals(expected, actual)
  }

  @Test def shouldSetColor() = {
    val expected = "rgba(127, 255, 0, 255)"
    val actual = geoJsonTools.setColor(50, Seq(0,25,75,100))
    Assert.assertEquals(expected, actual)
  }

  @Test def shouldCreatePolygon() = {
    val expected = "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[123.44500000000001,67.895],[123.455,67.895],[123.455,67.885],[123.44500000000001,67.885],[123.44500000000001,67.895]]]},\"properties\":{\"value\":1234567890,\"fill\":\"rgba(127, 255, 0, 255)\",\"fill-opacity\":0.5,\"stroke-width\":0}}"
    val actual = geoJsonTools.generatePolygon(
      new Coordinates(123.45, 67.89),
      "rgba(127, 255, 0, 255)",
      1234567890
    )
    Assert.assertEquals(expected, actual.toString)
  }

  @Test def testDFshouldContains105Elements = {
    Assert.assertEquals(105, testDF.count)
  }

  @Test def calculateTrafficVolumeShouldReturn81Elements = {
    val trafficVolume: DataFrame = Application.calculateTrafficVolume(testDF, parameters)
    Assert.assertEquals(81, trafficVolume.count)
  }

  @Test def geoJsonToolsShouldReturnProperJsonObject = {
    val trafficVolume: DataFrame = Application.calculateTrafficVolume(testDF, parameters)
    val trafficVolumeGeoJSON: JsonObject = geoJsonTools.convertToRectanglesHeatmap(trafficVolume)
    Assert.assertEquals(470799540, trafficVolumeGeoJSON.hashCode)
  }
}
