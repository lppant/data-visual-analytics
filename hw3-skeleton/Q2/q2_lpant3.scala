// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW
val lookupSchema = StructType(Array(StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))
val lookupDf = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true")
   .option("nullValue", "null")
   .schema(lookupSchema)
   .load("/FileStore/tables/taxi_zone_lookup.csv")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val dropoffDf = df_filter.groupBy($"DOLocationID").agg(expr("count(*) as number_of_dropoffs")).orderBy(col("number_of_dropoffs").desc , col("DOLocationID").asc)
dropoffDf.show(5)


// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val pickupDf = df_filter.groupBy($"PULocationID").agg(expr("count(*) as number_of_pickups")).orderBy(col("number_of_pickups").desc , col("PULocationID").asc)
pickupDf.show(5)

// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
val activityDf = dropoffDf.join(pickupDf,col("DOLocationID") === col("PULocationID")).withColumn("number_activities", col("number_of_dropoffs") + col ("number_of_pickups"))
val selectedActivityDf = activityDf.withColumnRenamed("PULocationID","LocationID").select("LocationID", "number_activities").orderBy(col("number_activities").desc ,col("LocationID").asc)
selectedActivityDf.show(3)

// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
val selectedActivityCopyDf = selectedActivityDf.withColumnRenamed("LocationID","LocationIDCopy")
val boroughActivityDf = selectedActivityCopyDf.join(lookupDf,col("LocationID") === col("LocationIDCopy")).select("Borough","number_activities").groupBy("Borough").sum().withColumnRenamed("sum(number_activities)","total_number_activities").orderBy(col("total_number_activities").desc)
boroughActivityDf.show()

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
val pickupDateDf = df_filter.withColumn("pickup_date", to_date(col("pickup_datetime"),"yyyy-MM-dd"))
val dateCountDf = pickupDateDf.groupBy("pickup_date").count()
val avgDailyStatsDf = dateCountDf.withColumn("day_of_week", date_format(col("pickup_date"), "EEEE")).groupBy("day_of_week").avg()
val finalStatsDf = avgDailyStatsDf.withColumnRenamed("avg(count)","avg_count").orderBy(col("avg_count").desc)
finalStatsDf.show(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
val pickupHourOfDayDf = df_filter.withColumn("hour_of_day", hour(col("pickup_datetime")))
val partitionBy = Window.partitionBy('hour_of_day).orderBy('count)
val brooklynZoneDf = pickupHourOfDayDf.join(lookupDf,col("LocationID") === col("PULocationID")).filter(col("Borough") === "Brooklyn").groupBy("hour_of_day","Zone").count()
val rankedDf = brooklynZoneDf.withColumn("rank", rank over partitionBy).orderBy(col("hour_of_day"), col("rank").desc)
val largestPickupDf = rankedDf.groupBy("hour_of_day").max("rank").withColumnRenamed("hour_of_day","temp_hour").join(rankedDf,col("temp_hour")===col("hour_of_day") && col("max(rank)") === col("rank"))
val formattedLargestPickupDf = largestPickupDf.withColumnRenamed("count","max_count").select("hour_of_day","Zone","max_count").orderBy("hour_of_day")
formattedLargestPickupDf.show(24, false)

// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW
val janPickupDf = df_filter.filter(month(col("pickup_datetime")) === 1).withColumn("day",dayofmonth(col("pickup_datetime"))).groupBy("day").agg(expr("count(*) as pickups"))
val windowBy = Window.orderBy("day")
val previousDayDf = janPickupDf.withColumn("previous_day_pickups",lag("pickups", 1).over(windowBy))
val percentDf = previousDayDf.withColumn("percent_diff", round(((col("pickups")-col("previous_day_pickups"))/col("previous_day_pickups") * 100), 2))
val finalDf = percentDf.withColumn("absolute_percent_diff", abs(col("percent_diff"))).orderBy(col("absolute_percent_diff").desc).select("day","percent_diff")
finalDf.show(10)
