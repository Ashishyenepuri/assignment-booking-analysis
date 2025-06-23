# Commercial Booking Analysis

## Overview
This batch job identifies the most popular **destination countries** for **KL flights** departing from the **Netherlands**, grouped by **season** and **day of the week**, within a specified **date range**. It is designed to support both **local execution** and **Hadoop-based large-scale processing**, helping KLM’s Network department make informed decisions about expanding their flight routes.
This batch job is implemented in scala 
## Features
-> Batch processing of booking data in JSON format
-> Airport-country mapping using an open-source dataset
-> Filtering for KL flights only
-> Filtering only `Confirmed` bookings
-> One passenger counted per flight leg
-> Date range filtering
-> Local timezone-based day of the week
-> Grouping by `season`, `day of the week`, and `destination country`
-> Sorted output by booking count
-> Supports local file paths or HDFS input
-> Graceful handling of invalid/corrupt JSON records
-> Designed to run on both small datasets locally and TB-scale datasets on YARN clusters

## Input Data
We can udnerstand about the input files from BOOKINGS.md and AIRPORTS.md files respectively.
### Booking.json
It containts the  IATA code of the departure airport, IATA code of the destination airport, UTC time of the flight departure, UTC time of the flight arrival, timestamp of the event, unique identifier of the passenger, age of the passenger, etc.

### Airports.dat
It contains data of the airports: Airport ID, Name, City, Country, IATA, ICAO, Latitude, Longitude, Altitude, Timezone, DST, Tz Database time zone, Type and Source of data

## Project Structure

├── build.sbt
├── project/
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── klm/
│                   └── analytics/
│                       └── PopularDestinations.scala
├── data/
|    └── bookings
|    |    ├── bookings.json
|    |    └── BOOKINGS.md
|    ├── airports
|         ├── airports.dat
|         └── AIRPOTS.md
└── top_destinations.csv  # Output

## Pre-requisites:
-> Java 8 or above
-> Scala 2.12+
-> Apache Spark 3.x
-> sbt (scala build tool)

### Ensure that Java 11 is installed:

Steps to install it:
-> Install it using: brew install openjdk@11
-> sudo ln -sfn /usr/local/opt/openjdk@11/libexec/openjdk.jdk \
/Library/Java/JavaVirtualMachines/openjdk-11.jdk
-> Get the path and copy it using the command: /usr/libexec/java_home -V
-> export JAVA_HOME= copied path
-> export PATH=$JAVA_HOME/bin:$PATH
-> Verify that you have Java 11 installed by checking it's version: export PATH=$JAVA_HOME/bin:$PATH


## How to Run the batch job?
The batch job is designed to be easy to run- locally for development and on production hadoop clusters.

We first compile it using the command:
sbt compile 

To execute it locally, give the command:
sbt "run data/stream_sample/booking.json data/stream_sample/airports.dat <start_date> <end_date>"

This command takes 4 arguments path to the bookings JSON file, path to the airport metadata file, start date, and end date.
There isn't any other additional configuration required Spark, Scala, and sbt being installed.




## Output 
The output is saved as top_destinations.csv file.
It has the columns: season, dayOfweek, destination_country, booking_count

The csv file is sorted by:
Booking Count (descending order)
Season (Winter -> Autumn)
Day of Week (Monday -> Sunday)



## Project Assunptions and Design Choices
-> Corrupt JSON file handling: Filtered using _corrupt_record handling.
-> Confirmed bookings only: Latest status is used.
-> Unique passenger per flight leg: Achieved by deduplicating at the flight level
-> Timezone: Assumed all departure timestamps are in local time.
-> Scalability: Code is optimized to handle TB-scale data using Spark's distributed model.
-> Clean and Structured Output: Output is coalesced to a single CSV file and renamed for usability.
