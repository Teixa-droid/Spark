package com.bigdata.spark01
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main extends App {

  // Set up the SparkSession
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .appName("SparkChallenge")
    .master("local[1]") // number of threads as needed
    .getOrCreate()

  // Define paths to CSV files
  val gpsUserReviewsPath = "data/googleplaystore_user_reviews.csv"
  val gpsPath = "data/googleplaystore.csv"

  // Read the CSV files into DataFrames
  val userReviewsDF = readCsv(gpsUserReviewsPath)
  val gpsDF = readCsv(gpsPath)

  // Define the output paths
  val outputPath = "data/output/"
  val bestAppsOutputPath = outputPath + "best_apps.csv"
  val combinedOutputPath = outputPath + "googleplaystore_cleaned"
  val genreMetricsOutputPath = outputPath + "googleplaystore_metrics"

  // Function to read CSV files
  def readCsv(path: String): DataFrame = {
    spark.read.option("header", value = true).csv(path)
  }

  // Function to save a DataFrame as a Parquet file with gzip compression
  def saveAsParquetWithGzip(df: DataFrame, outputPath: String): Unit = {
    df.write.option("compression", "gzip").parquet(outputPath)
  }

  // Part 1: Calculate Average Sentiment by App
  val avgSentimentByAppDF = calculateAverageSentimentByApp()
  println("Part 1: Calculate Average Sentiment by App")
  avgSentimentByAppDF.show()

  // Part 2: Find Best Rated Apps and Save to CSV
  val bestRatedAppsDF = findBestRatedApps()
  println("Part 2: Find Best Rated Apps and Save to CSV")
  saveAsCsv(bestRatedAppsDF, bestAppsOutputPath)

  // Part 3: Clean and Transform Data
  val cleanedDF = cleanAndTransformData()
  println("Part 3: Clean and Transform Data")
  cleanedDF.show()

  // Part 4: Combine Exercise 1 and Exercise 3 DataFrames
  val combinedDF = combineDataFrames(cleanedDF, avgSentimentByAppDF)
  println("Part 4: Combine Exercise 1 and Exercise 3 DataFrames")
  combinedDF.show(100)

  //Save Combined DataFrame as Parquet
  println("Part 5: Save Combined DataFrame as Parquet")
  saveAsParquetWithGzip(combinedDF, combinedOutputPath)

  // Part 5: Calculate Metrics by Genre and Save as Parquet
  val genreMetricsDF = calculateAndSaveGenreMetrics(combinedDF)
  println("Part 6: Calculate Metrics by Genre and Save as Parquet")
  genreMetricsDF.show()
  saveAsParquetWithGzip(genreMetricsDF, genreMetricsOutputPath)

  // Function to calculate average sentiment by app
  def calculateAverageSentimentByApp(): DataFrame = {
    // Clean and cast Sentiment_Polarity
    val filteredUserReviewsDF = userReviewsDF
      .filter(col("Sentiment_Polarity").isNotNull)
      .filter(col("Sentiment_Polarity").rlike("-?[0-9]+(\\.[0.9]+)?"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("float"))

    // Calculate average sentiment by app
    val avgSentimentByAppDF = filteredUserReviewsDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    avgSentimentByAppDF
  }

  // Function to find best-rated apps
  def findBestRatedApps(): DataFrame = {
    gpsDF.filter(col("Rating") >= 4.0)
      .orderBy(col("Rating").desc)
  }

  // Function to save a DataFrame as a CSV file with § as delimiter
  def saveAsCsv(df: DataFrame, outputPath: String): Unit = {
    df.write.option("header", value = true)
      .option("delimiter", "§")
      .csv(outputPath)
  }

  // Function to clean and transform the Google Play Store data
  def cleanAndTransformData(): DataFrame = {
    // Data cleaning and transformation
    val cleanedDF = gpsDF
      .withColumn("Genres", split(col("Genres"), ";"))
      .withColumn("Rating", col("Rating").cast("float"))
      .withColumn("Reviews", col("Reviews").cast("long"))
      .withColumn("Price", (regexp_replace(col("Price"), "\\$", "").cast("double") * 0.9))
      .withColumn("Size", when(col("Size").rlike("k|K"),
        regexp_replace(col("Size"), "[a-zA-Z]+", "").cast("double") / 1024)
        .otherwise(regexp_replace(col("Size"), "[a-zA-Z]+", "").cast("double")))
      .withColumn("Last_Updated", to_date(col("Last Updated"), "MMMM d, yyyy"))
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")

    // Group and aggregate data
    val aggregatedDF = cleanedDF
      .groupBy("App")
      .agg(
        collect_list("Category").alias("Categories"),
        max("Rating").alias("Rating"),
        max("Reviews").alias("Reviews"),
        max("Size").alias("Size"),
        first("Installs").alias("Installs"),
        first("Type").alias("Type"),
        max("Price").alias("Price"),
        first("Content_Rating").alias("Content_Rating"),
        first("Genres").alias("Genres"),
        max("Last_Updated").alias("Last_Updated"),
        first("Current_Version").alias("Current_Version"),
        first("Minimum_Android_Version").alias("Minimum_Android_Version")
      )

    aggregatedDF
  }

  // Function to combine Exercise 1 and Exercise 3 DataFrames
  def combineDataFrames(cleanedDF: DataFrame, avgSentimentByAppDF: DataFrame): DataFrame = {
    cleanedDF.join(avgSentimentByAppDF, Seq("App"), "left")
  }

  // Function to calculate metrics by genre
  def calculateAndSaveGenreMetrics(combinedDF: DataFrame): DataFrame = {
    // Explode the 'Genres' array column to get one row per genre
    val explodedDF = combinedDF.withColumn("Genre", explode(col("Genres")))

    // Group by 'Genre' and calculate the metrics
    val genreMetricsDF = explodedDF.groupBy("Genre")
      .agg(
        count("App").alias("Num_Applications"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    genreMetricsDF
  }
}
