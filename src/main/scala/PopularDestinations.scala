package com.klm.analytics

// Importing necessary packages
import org.apache.spark.sql.{SparkSession, functions => F, DataFrame, Column, Row, Dataset}
import org.apache.spark.sql.expressions.Window

object PopularDestinations {
  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Usage: PopularDestinations <booking.json> <airports.dat> <start-date> <end-date>") // Ensuring that we give the correct 4 arguments

    val spark = SparkSession.builder() // Initiliasing of Spark session
      .appName("KLM Popular Destinations")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input arguments
    val bookingPath = args(0)
    val airportsPath = args(1)
    val startDate = args(2)
    val endDate = args(3)


    // Reading Airport data, parsing with no header and comma separated
    val airportsRaw = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .option("delimiter", ",")
      .csv(airportsPath)
      .toDF("AirportID", "Name", "City", "Country", "IATA", "ICAO",
            "Latitude", "Longitude", "Altitude", "Timezone", "DST",
            "TzDatabaseTimezone", "Type", "Source")
      .filter(F.col("IATA").isNotNull && F.col("Country").isNotNull)

    // Extract and clean origin airport codes and countries
    val airportOrigin = airportsRaw
      .select(F.upper(F.trim(F.col("IATA"))).alias("origin"), F.lower(F.trim(F.col("Country"))).alias("origin_country"))
      .distinct()
    // Extract and clean destination airport codes and countries
    val airportDest = airportsRaw
      .select(F.upper(F.trim(F.col("IATA"))).alias("destination"), F.lower(F.trim(F.col("Country"))).alias("destination_country"))
      .distinct()

    // Handling corrupt or invalid records in JSON file
    val rawJson = spark.read
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .json(bookingPath)

    // Filtering out corrupt records
    val bookingsRaw = if (rawJson.columns.contains("_corrupt_record")) {
      rawJson.filter(F.col("_corrupt_record").isNull).drop("_corrupt_record")
    } else {
      rawJson
    }

    // Explode the nested structure to extract flight-level booking information
    val exploded = bookingsRaw
      .withColumn("product", F.explode(F.col("event.DataElement.travelrecord.productsList")))
      .select(
        F.col("product.flight.operatingAirline").alias("carrier"),
        F.col("product.flight.originAirport").alias("origin"),
        F.col("product.flight.destinationAirport").alias("destination"),
        F.col("product.flight.departureDate").alias("departureDate"),
        F.col("product.bookingStatus").alias("bookingStatus")
      )

    // Filtering KLM (KL) flights and clean airport records
    val filtered = exploded
      .filter(
        F.upper(F.col("carrier")) === "KL" &&
        F.upper(F.col("bookingStatus")) === "CONFIRMED"
      )
      .withColumn("origin", F.upper(F.trim(F.col("origin"))))
      .withColumn("destination", F.upper(F.trim(F.col("destination"))))

    // Join with origin airport info and filter only flights from the Netherlands
    val joined = filtered
      .join(airportOrigin, Seq("origin"), "inner")
      .filter(F.col("origin_country") === "netherlands")
      .withColumn("departureDate", F.to_timestamp(F.col("departureDate")))
      .filter(F.col("departureDate").between(F.lit(startDate), F.lit(endDate)))

    // Add season and day-of-week fields to analyze temporal trends
    val enriched = joined
      .withColumn("dayOfWeek", F.date_format(F.col("departureDate"), "EEEE"))
      .withColumn("month", F.month(F.col("departureDate")))
      .withColumn("season",
        F.when(F.col("month").isin(12, 1, 2), "Winter")
         .when(F.col("month").isin(3, 4, 5), "Spring")
         .when(F.col("month").isin(6, 7, 8), "Summer")
         .when(F.col("month").isin(9, 10, 11), "Autumn")
         .otherwise("Unknown")
      )
      .drop("month")

    //Join with destination airport info and group by season/day/country to count bookings
    val grouped = enriched
      .join(airportDest, Seq("destination"), "inner")
      .groupBy("season", "dayOfWeek", "destination_country")
      .agg(F.count("destination").alias("booking_count"))


    val seasonOrder = F.when(F.col("season") === "Winter", 1)
      .when(F.col("season") === "Spring", 2)
      .when(F.col("season") === "Summer", 3)
      .when(F.col("season") === "Autumn", 4)
      .otherwise(5)

    val weekdayOrder = F.when(F.col("dayOfWeek") === "Monday", 1)
      .when(F.col("dayOfWeek") === "Tuesday", 2)
      .when(F.col("dayOfWeek") === "Wednesday", 3)
      .when(F.col("dayOfWeek") === "Thursday", 4)
      .when(F.col("dayOfWeek") === "Friday", 5)
      .when(F.col("dayOfWeek") === "Saturday", 6)
      .when(F.col("dayOfWeek") === "Sunday", 7)
      .otherwise(8)

    // Sort results grouped by season and day-of-week, descending by booking count
    val sorted = grouped
      .withColumn("season_order", seasonOrder)
      .withColumn("weekday_order", weekdayOrder)
      .orderBy(F.col("season_order"), F.col("weekday_order"), F.col("booking_count").desc)
      .drop("season_order", "weekday_order")

    sorted.show(100, true)

    // Writing the result to a single CSV file
    sorted.coalesce(1).write
      .option("header", "true")
      .mode("overwrite")
      .csv("top_destinations_temp")

    // Renaming the CSV file to a fixed file name
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tempDirPath = new org.apache.hadoop.fs.Path("top_destinations_temp")
    val finalPath = new org.apache.hadoop.fs.Path("top_destinations.csv")
    fs.delete(finalPath, true)

    val csvFile = fs.listStatus(tempDirPath).filter(_.getPath.getName.endsWith(".csv")).head.getPath
    fs.rename(csvFile, finalPath)
    fs.delete(tempDirPath, true)

    spark.stop()
  }
}
