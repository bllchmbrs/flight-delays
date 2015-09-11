import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object FlightDelays extends Serializable {

  def convertColumns(df: org.apache.spark.sql.DataFrame, colTypeMap: Map[String, String]) = {
    var localDf = df
    for (Tuple2(column, newType) <- colTypeMap.iterator) {
      localDf = localDf.withColumn(column, localDf.col(column).cast(newType))
    }
    localDf
  }

  def cleanColumnNames(df: org.apache.spark.sql.DataFrame, sqlContext: org.apache.spark.sql.SQLContext) = {
    // courtesy
    // https://issues.apache.org/jira/browse/SPARK-2775
    // http://stackoverflow.com/questions/30359539/accessing-column-names-with-periods-spark-sql-1-3
    sqlContext.createDataFrame(df.rdd,
      org.apache.spark.sql.types.StructType(df.schema.fields.map(sf =>
        org.apache.spark.sql.types.StructField(sf.name
          .replace(".", "").replace(" ","")
          .replace("(","").replace(")","").replace("%",""), sf.dataType, sf.nullable)
      ))
    )
  }

  def generateLag(data: DataFrame, colName: String, nRows: Int) = {
    val s = s"lag$colName$nRows"
    val lagWindow = Window
      .partitionBy("CallSign")
      .orderBy(data.col("YearMonthDay"), data.col("Time"))

    lag(data.col(colName), nRows).over(lagWindow).alias(s)
  }

  def generateSum(data: DataFrame, colName: String, nRows:Int) = {
    val s = s"sum$colName$nRows"
    val sumWindow1 = Window
      .partitionBy("CallSign")
      .orderBy("YearMonthDay", "Time")
      .rowsBetween(-1 * nRows,0)

      sum(data.col(colName)).over(sumWindow1).alias(s)
  }

  def loadWeatherData(sqlContext: org.apache.spark.sql.SQLContext) = {

    // load up the data!
    var rawWeatherData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/weather_data/*hourly.txt")
      .repartition(36)
    val newWeatherTypes = Map(("WindSpeedkt", "Float"), ("DryBulbTemp", "Float"),
      ("DewPointTemp", "Float"), ("WetBulbTemp", "Float"),("PrecipTotal", "Float"))

    rawWeatherData = cleanColumnNames(rawWeatherData,sqlContext)
      .withColumnRenamed("WbanNumber", "ID")

    rawWeatherData = convertColumns(rawWeatherData, newWeatherTypes)

    val rawStationList = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter","|")
      .load("s3n://b-datasets/weather_data/*station.txt")
      .distinct
      .select("Call Sign","WBAN Number")
      .withColumnRenamed("WBAN Number", "ID1")
      .withColumnRenamed("Call Sign", "CallSign")
    // we are assuming that the station names/locations haven't changed since we're grabbing distinct

    rawWeatherData
      .join(rawStationList,
        rawWeatherData.col("ID") === rawStationList.col("ID1"), "outer")
      .drop("ID")
      .drop("ID1")
      .drop("StationType")
      .drop("MaintenanceIndicator")
      .drop("SkyConditions")
      .drop("Visibility")
      .drop("WeatherType")
      .drop("RelativeHumidity")
      .drop("WindDirection")
      .drop("WindCharGustskt")
      .drop("ValforWindChar")
      .drop("StationPressure")
      .drop("PressureTendency")
      .drop("SeaLevelPressure")
      .drop("RecordType")
      .repartition(36)
  }

  def loadFlightData(sqlContext: org.apache.spark.sql.SQLContext) = {

    val rawFlightData = sqlContext
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/flight_data/*")

    val newFlightTypes = Map(
      ("Year", "int"), ("Month", "int"), ("DayofMonth","int"),
      ("DayOfWeek","int"), ("WeatherDelay", "int")
    )

    // just remove all these extra columns to keep things neat
    convertColumns(rawFlightData, newFlightTypes)
      .drop("LateAircraftDelay")
      .drop("SecurityDelay")
      .drop("NASDelay")
      .drop("CarrierDelay")
      .drop("Diverted")
      .drop("CancellationCode")
      .drop("Cancelled")
      .drop("TaxiOut")
      .drop("TaxiIn")
      .drop("AirTime")
      .drop("CRSElapsedTime")
      .drop("ActualElapsedTime")
      .drop("ArrTime")
      .drop("DepTime")
      .drop("UniqueCarrier")
      .drop("FlightNum")
      .drop("TailNum")
      .drop("ArrDelay")
      .drop("DepDelay")
      .drop("Distance")
      .repartition(36)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("FlightDelayPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  var weatherData = loadWeatherData(sqlContext)
  var flightData = loadFlightData(sqlContext)
  weatherData.write.parquet("weatherData.parquet")
  flightData.write.parquet("flightData.parquet")
  weatherData = sqlContext.read.parquet("weatherData.parquet").repartition(24)
  flightData = sqlContext.read.parquet("flightData.parquet").repartition(24)
  weatherData.cache()
  flightData.cache()
  weatherData.registerTempTable("rawWeather")
  flightData.registerTempTable("rawFlight")



    weatherData.select(
      $"CallSign",
      $"YearMonthDay",
      $"Time",
      $"WindSpeedkt",
      generateLag(weatherData, "WindSpeedkt", 1),
      generateSum(weatherData, "WindSpeedkt", 1),
      generateLag(weatherData, "WindSpeedkt", 3),
      generateSum(weatherData, "WindSpeedkt", 3),
      generateLag(weatherData, "WindSpeedkt", 6),
      generateSum(weatherData, "WindSpeedkt", 6),
      generateLag(weatherData, "WindSpeedkt", 12),
      generateSum(weatherData, "WindSpeedkt", 12),
      generateLag(weatherData, "WindSpeedkt", 24),
      generateSum(weatherData, "WindSpeedkt", 24),
      generateLag(weatherData, "WindSpeedkt", 36),
      generateSum(weatherData, "WindSpeedkt", 36),
      generateLag(weatherData, "WindSpeedkt", 72),
      generateSum(weatherData, "WindSpeedkt", 72)
  ).show




  sqlContext.sql("""SELECT CallSign, PrecipTotal, sum(PrecipTotal) over w from rawWeather;
WINDOW w as (PARTITION BY (YearMonthDay, Time) ORDER BY (YearMonthDay, Time) ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)""")


  // need to convert to LabeledPoint in MLLib
  // we might need a hashingTF to convert the origins/destinations or something like that. Not exactly sure how to handle it.
  // this is going to be a like a one hot encoder or something

}
}
