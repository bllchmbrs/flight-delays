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
      .orderBy(data.col("Date"))

    lag(data.col(colName), nRows).over(lagWindow).alias(s)
  }

  def generateSum(data: DataFrame, colName: String, nRows:Int) = {
    val s = s"sum$colName$nRows"
    val sumWindow = Window
      .partitionBy("CallSign")
      .orderBy(data.col("Date"))
      .rowsBetween(-1 * nRows,0)

    sum(data.col(colName)).over(sumWindow).alias(s)
  }

  def generateAvg(data: DataFrame, colName: String, nRows:Int) = {
    val s = s"sum$colName$nRows"
    val sumWindow = Window
      .partitionBy("CallSign")
      .orderBy(data.col("Date"))
      .rowsBetween(-1 * nRows,0)

    avg(data.col(colName)).over(sumWindow).alias(s)
  }

  def loadWeatherData(sqlContext: org.apache.spark.sql.SQLContext) = {

    val makeDate = udf( (ymd: String, time:String, tz:String) => {
      val y = ymd.slice(0,4)
      val m = ymd.slice(4,6)
      val d = ymd.slice(6,8)
      val h = time.slice(0,2)
      val mm = time.slice(2,4)
      val hrs = tz.split("\\.")(0).toFloat
      val mins = ("0." + tz.split("\\.")(1)).toFloat * 60
      s"$y-$m-$d" + "T" + s"$h:$mm" + "+" + hrs + ":" + mins
    })

    var rawWeatherData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/weather_data/*hourly.txt")

    var airportMeta = sqlContext.read.format("com.databricks.spark.csv").load("s3n://b-datasets/airports.csv")

    Array("ID","NAME","CITY","COUNTRY","FAA","ICAO","LAT","LON","ALT","TZ","DST","TZ2").zipWithIndex.foreach { x =>
      val (name, cur) = x
      airportMeta = airportMeta.withColumnRenamed(s"C$cur", name)
    }

    rawWeatherData = cleanColumnNames(rawWeatherData, sqlContext)
      .withColumnRenamed("WbanNumber", "ID")
    
    val newWeatherTypes = Map(("WindSpeedkt", "Float"), ("DryBulbTemp", "Float"),
      ("DewPointTemp", "Float"), ("WetBulbTemp", "Float"),("PrecipTotal", "Float"))

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

    rawWeatherData = rawWeatherData
      .join(rawStationList,
        rawWeatherData.col("ID") === rawStationList.col("ID1"), "outer")

    rawWeatherData
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
      .repartition(24)
  }

  def loadFlightData(sqlContext: org.apache.spark.sql.SQLContext) = {

    val makeDate = udf( (y: String, m:String, d:String, time:String, tz:String) => {
      val h = time.slice(0,2)
      val mm = time.slice(2,4)
      val hrs = tz.split("\\.")(0).toFloat
      val mins = ("0." + tz.split("\\.")(1)).toFloat * 60
      s"$y-$m-$d" + "T" + s"$h:$mm" + "+" + hrs + ":" + mins
    })

    var rawFlightData = sqlContext
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/flight_data/*")

    val newFlightTypes = Map(("WeatherDelay", "int"))

    var airportMeta = sqlContext.read.format("com.databricks.spark.csv").load("s3n://b-datasets/airports.csv")

    Array("ID","NAME","CITY","COUNTRY","FAA","ICAO","LAT","LON","ALT","TZ","DST","TZ2").zipWithIndex.foreach { x =>
      val (name, cur) = x
      airportMeta = airportMeta.withColumnRenamed(s"C$cur", name)
    }

    // just remove all these extra columns to keep things neat
    rawFlightData = convertColumns(rawFlightData, newFlightTypes)
      .join(airportMeta.select($"FAA",$"TZ"),
        airportMeta.col("FAA") === rawFlightData.col("Origin"), "outer")

    rawFlightData
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
      .drop("DayOfWeek")
      .repartition(24)
  }

  // def main(args: Array[String]) = {
  //   val conf = new SparkConf().setAppName("FlightDelayPredictor")
  //   val sc = new SparkContext(conf)
  //   val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    var weatherData = loadWeatherData(sqlContext)
    var flightData = loadFlightData(sqlContext)
    weatherData.write.parquet("weatherData.parquet")
    flightData.write.parquet("flightData.parquet")
    var weatherData = sqlContext.read.parquet("weatherData.parquet").repartition(24)
    var flightData = sqlContext.read.parquet("flightData.parquet").repartition(24)
    weatherData.cache()
    flightData.cache()
    weatherData.registerTempTable("rawWeather")
    flightData.registerTempTable("rawFlight")




    weatherData.select(
      $"CallSign",
      $"Date",
      $"WindSpeedkt",
      $"PrecipTotal",
      $"DryBulbTemp",
      $"DewPointTemp",
      $"WetBulbTemp",
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
      generateSum(weatherData, "WindSpeedkt", 72),
      generateLag(weatherData, "PrecipTotal", 1),
      generateSum(weatherData, "PrecipTotal", 1),
      generateLag(weatherData, "PrecipTotal", 3),
      generateSum(weatherData, "PrecipTotal", 3),
      generateLag(weatherData, "PrecipTotal", 6),
      generateSum(weatherData, "PrecipTotal", 6),
      generateLag(weatherData, "PrecipTotal", 12),
      generateSum(weatherData, "PrecipTotal", 12),
      generateLag(weatherData, "PrecipTotal", 24),
      generateSum(weatherData, "PrecipTotal", 24),
      generateLag(weatherData, "PrecipTotal", 36),
      generateSum(weatherData, "PrecipTotal", 36),
      generateLag(weatherData, "PrecipTotal", 72),
      generateSum(weatherData, "PrecipTotal", 72),
      generateAvg(weatherData, "DryBulbTemp", 1),
      generateAvg(weatherData, "DryBulbTemp", 3),
      generateAvg(weatherData, "DryBulbTemp", 6),
      generateAvg(weatherData, "DryBulbTemp", 12),
      generateAvg(weatherData, "DryBulbTemp", 24),
      generateAvg(weatherData, "DryBulbTemp", 36),
      generateAvg(weatherData, "DryBulbTemp", 72),
      generateAvg(weatherData, "DewPointTemp", 1),
      generateAvg(weatherData, "DewPointTemp", 3),
      generateAvg(weatherData, "DewPointTemp", 6),
      generateAvg(weatherData, "DewPointTemp", 12),
      generateAvg(weatherData, "DewPointTemp", 24),
      generateAvg(weatherData, "DewPointTemp", 36),
      generateAvg(weatherData, "DewPointTemp", 72),
      generateAvg(weatherData, "WetBulbTemp", 1),
      generateAvg(weatherData, "WetBulbTemp", 3),
      generateAvg(weatherData, "WetBulbTemp", 6),
      generateAvg(weatherData, "WetBulbTemp", 12),
      generateAvg(weatherData, "WetBulbTemp", 24),
      generateAvg(weatherData, "WetBulbTemp", 36),
      generateAvg(weatherData, "WetBulbTemp", 72)
    ).show



    // need to convert to LabeledPoint in MLLib

  }
}
