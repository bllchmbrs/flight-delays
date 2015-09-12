import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{lag, avg, sum, udf}
import org.apache.spark.sql.expressions.Window

object FlightDelays extends Serializable {

  def convertColumns(df: DataFrame, colTypeMap: Map[String, String]) = {
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

    var rawWeatherData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/weather_data/*hourly.txt")

    rawWeatherData = cleanColumnNames(rawWeatherData, sqlContext)
      .withColumnRenamed("WbanNumber", "ID")
    
    val newWeatherTypes = Map(("WindSpeedkt", "Float"), ("DryBulbTemp", "Float"),
      ("DewPointTemp", "Float"), ("WetBulbTemp", "Float"),("PrecipTotal", "Float"))

    convertColumns(rawWeatherData, newWeatherTypes)
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
  }

  def loadFlightData(sqlContext: org.apache.spark.sql.SQLContext) = {

    var rawFlightData = sqlContext
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/flight_data/*")

    val newFlightTypes = Map(("WeatherDelay", "int"))

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
      .drop("DayOfWeek")
  }

  def loadAirportMeta(sqlContext: org.apache.spark.sql.SQLContext) = {

    var airportMeta = sqlContext.read.format("com.databricks.spark.csv").load("s3n://b-datasets/airports.csv")
    Array("ID","NAME","CITY","COUNTRY","FAA","ICAO","LAT","LON","ALT","TZ","DST","TZ2").zipWithIndex.foreach { x =>
      val (name, cur) = x
      airportMeta = airportMeta.withColumnRenamed(s"C$cur", name)
    }
    airportMeta.select($"FAA",$"TZ")
  }

  
  def processHr(hr:String) = {
    if (hr.length == 3) hr
    else if (hr.length == 2 && !hr.charAt(0).isDigit) hr.charAt(0) + "0" + hr.charAt(1)
    else if (hr.length == 2) "+" + hr
    else "+0" + hr
  }

  def processMin(min:String) = if (min.length == 1) "0" + min else min

  def toUTCFmt(tz: String) = {
    val splitted = tz.split("\\.")
    if (splitted.length == 2) {
      processHr(splitted(0)) +  ":" + processMin((splitted(1).toInt * 6).toString)
    } else {
      processHr(splitted(0))
    }
  }



  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("FlightDelayPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   
    val mdt1 = udf((ymd: String, time:String, tz:String) => {
      val y = ymd.slice(0,4)
      val m = ymd.slice(4,6)
      val d = ymd.slice(6,8)
      val h = time.slice(0,2)
      val mm = time.slice(2,4)
      val utc = utcAbbrs.value(toUTCFmt(tz)).toString
      s"$y-$m-$d " + s"$h:$mm " + s"$utc"
    })

    val mdt2 = udf((y: String, m:String, d:String, time:String, tz:String) => {
      val h = time.slice(0,2)
      val mm = time.slice(2,4)
      val utc = utcAbbrs.value(toUTCFmt(tz)).toString
      s"$y-$m-$d " + s"$h:$mm " + s"$utc"
    })

    val utcabbr = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/utcabbrs.csv")
    val utcAbbrs = sc.broadcast(Map[String, String]() ++ utcabbr.map(x => (x(1), x(0))).collect)


    // load up our broadcast variables
    val stationMeta = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter","|")
      .load("s3n://b-datasets/weather_data/*station.txt")
      .distinct
      .select("Call Sign","WBAN Number")
      .withColumnRenamed("WBAN Number", "ID1")
      .withColumnRenamed("Call Sign", "CallSign")

    var airportMeta = loadAirportMeta(sqlContext)
    airportMeta = airportMeta
      .join(utcabbr, airportMeta.col("FAA") === utcabbr.col("Abbr"), "inner")
    airportMeta = airportMeta
      .join(stationMeta, airportMeta.col("FAA") === stationMeta.col("CallSign"), "inner")
      .drop("FAA")
      .drop("Abbr")
      .cache()


    val dtfmt = "yyyy-MM-dd hh:mm z"

    // Load it into hdfs for faster usage
    var weatherData = loadWeatherData(sqlContext)
    var flightData = loadFlightData(sqlContext)
    weatherData.repartition(12).write.parquet("weatherData.parquet")
    flightData.repartition(12).write.parquet("flightData.parquet")

    weatherData = sqlContext.read.parquet("weatherData.parquet").repartition(24)
    weatherData.cache()
    weatherData.registerTempTable("rawWeather")
    flightData = sqlContext.read.parquet("flightData.parquet").repartition(24)
    flightData.cache()
    flightData.registerTempTable("rawFlight")

    var fd = flightData
    fd = fd
      .join(airportMeta, fd.col("Origin") === airportMeta.col("CallSign"), "inner")
    fd= fd
    .withColumn("d2", mdt2(fd.col("Year"), fd.col("Month"), fd.col("DayofMonth"), fd.col("CRSDepTime"), fd.col("TZ")))








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
