import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.functions._

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
        org.apache.spark.sql.types.StructField(sf.name.replace(".", "").replace(" ",""), sf.dataType, sf.nullable)
      ))
    )
  }

  def genSql(func:String, partitionBy: String, orderBy: String, hours: Int) = {
    (field:String) => {
      s" $func(float($field), $hours, 0) over (partition by $partitionBy order by $orderBy) as lag$hours$field "
    }
  }

  def loadWeatherData(sqlContext: org.apache.spark.sql.SQLContext) = {

    // load up the data!
    val rawWeatherData1 = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/weather_data/*hourly.txt")
      .repartition(36)

    val rawWeatherData = cleanColumnNames(rawWeatherData1,sqlContext)
      .withColumnRenamed("WbanNumber", "ID")

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
      .repartition(36)
  }

  def loadFlightData(sqlContext: org.apache.spark.sql.SQLContext) = {

    val rawFlightData = sqlContext
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/flight_data/*")
      .repartition(36)

    val newFlightTypes = Map(
      ("Year", "int"), ("Month", "int"), ("DayofMonth","int"),
      ("DayOfWeek","int"), ("ActualElapsedTime","int"),("CRSElapsedTime", "int"),
      ("AirTime","int"),("ArrDelay","int"),("DepDelay","int"),("Distance","int"),
      ("TaxiIn","int"),("TaxiOut","int"), ("CarrierDelay", "int"),
      ("WeatherDelay", "int"), ("NASDelay","int"), ("SecurityDelay","int"),
      ("LateAircraftDelay","int")
    )

    val rawFlightData2 = convertColumns(rawFlightData, newFlightTypes)

    rawFlightData2
      .withColumn("TotalDelay", rawFlightData2.col("ArrDelay") + rawFlightData2.col("DepDelay"))
      .drop("LateAircraftDelay")
      .drop("SecurityDelay")
      .drop("NASDelay")
      .drop("WeatherDelay")
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
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("FlightDelayPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val weatherData = loadWeatherData(sqlContext).cache()
    val flightData = loadFlightData(sqlContext).cache()
    weatherData.registerTempTable("rawWeather")
    flightData.registerTempTable("rawFlight")

    //lag example
    sqlContext.sql("""SELECT CallSign, int(PrecipTotal), lag(int(PrecipTotal),1,0)
    over (partition by CallSign order by YearMonthDay) as lagprecip 
    FROM rawWeather 
    WHERE PrecipTotal != null""")
      .take(400)

    // generate our functions, this is a bit hacky but it works :)
    val lag1hr = genSql("lag", "CallSign", "(int(YearMonthDay), int(Time))", 1)
    val lag3hr = genSql("lag", "CallSign", "(int(YearMonthDay), int(Time))", 3)
    val lag6hr = genSql("lag", "CallSign", "(int(YearMonthDay), int(Time))", 6)
    val lag12hr = genSql("lag", "CallSign", "(int(YearMonthDay), int(Time))", 12)

    val sum1hr = genSql("sum", "CallSign", "(int(YearMonthDay), int(Time))", 1)
    val sum3hr = genSql("sum", "CallSign", "(int(YearMonthDay), int(Time))", 3)
    val sum6hr = genSql("sum", "CallSign", "(int(YearMonthDay), int(Time))", 6)
    val sum12hr = genSql("sum", "CallSign", "(int(YearMonthDay), int(Time))", 12)

    val weatherManip = Array(
      "int(trim(regexp_replace(Visibility, 'SM','')))",
      "int(WeatherType",
      "int(DryBulbTemp)",
      "int(DewPointTemp)",
      "int(WetBulbTemp)",
      "int(%RelativeHumidity)")

    val weatherLagSum = weatherManip.map(lag1hr) ++ weatherManip.map(lag3hr) ++
    weatherManip.map(lag6hr) ++ weatherManip.map(lag12hr) ++
    weatherManip.map(sum1hr) ++ weatherManip.map(sum3hr) ++
    weatherManip.map(sum6hr) ++ weatherManip.map(sum12hr)

    sqlContext.sql("SELECT CallSign, " + (weatherLagSum).mkString(", ") + ", CallSign FROM rawWeather")
      .registerTempTable("weather")
    // is this automatically query optimized?



    sqlContext.sql("SELECT * FROM flight LEFT JOIN weather ON (flight. == weather.ID)").registerTempTable("base")



    // need to convert to LabeledPoint in MLLib
    // we might need a hashingTF to convert the origins/destinations or something like that. Not exactly sure how to handle it.
    // this is going to be a like a one hot encoder or something

  }
}
