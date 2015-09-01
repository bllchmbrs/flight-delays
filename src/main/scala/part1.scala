import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vector

object FlightDelays extends Serializable {

  def convertColumns(df: org.apache.spark.sql.DataFrame, colTypeMap: Map[String, String]) = {
    var localDf = df
    for (Tuple2(column, newType) <- colTypeMap.iterator) {
      localDf = localDf.withColumn(column, localDf.col(column).cast(newType))
    }
    localDf
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("FlightDelayPredictor")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // load up the data!
    val rawWeatherData = sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")
      .load("s3n://b-datasets/weather_data/*hourly.txt")
      .repartition(36)

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

    val flights = convertColumns(rawFlightData, newFlightTypes).cache()


    // we've got some columns that we're going to need to remove because they tell us the future
    // like diverted is an issue
    // Drop cancelled, cancellation code, diverted, all the delays, the elapsed time features unless we aggregate them
    // actual elapsed times, the air times
    // think our core predictor should be the total delay but we shouldn't be able to account for the departure delay


    val splits = flights.randomSplit(Array(0.6,0.4),seed=11L)
    val train = splits(0)
    val test = splits(1)
    // need to convert to LabeledPoint in MLLib
    // we might need a hashingTF to convert the origins/destinations or something like that. Not exactly sure how to handle it.
    // this is going to be a like a one hot encoder or something

  }
}
