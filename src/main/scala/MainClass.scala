import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MainClass {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("Sentiment Analysis with Scala and Spark")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")  // Example configuration, adjust as needed
      .master("local[*]")  // Run Spark locally with all available cores
      .getOrCreate()

    // Define the schema for the CSV file
    val schema = StructType(Seq(
      StructField("textID", StringType, nullable = true),
      StructField("text", StringType, nullable = true),
      StructField("sentiment", StringType, nullable = true),
      StructField("Time of Tweet", StringType, nullable = true),
      StructField("Age of User", StringType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("Population -2020", LongType, nullable = true),
      StructField("Land Area (Km²)", LongType, nullable = true),
      StructField("Density (P/Km²)", LongType, nullable = true)
    ))

    // Read CSV data into DataFrame
    val df = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\test.csv")

    // Define a function to determine sentiment based on text
    def getSentiment(text: String): String = {
      // Check for null input and handle it
      if (text == null) "neutral"
      else {
        // Implement your sentiment analysis logic here
        // Example: For simplicity, let's assume basic rules for demonstration
        if (text.contains("good") || text.contains("happy") || text.contains("great")) "positive"
        else if (text.contains("bad") || text.contains("sad") || text.contains("terrible")) "negative"
        else "neutral"
      }
    }

    // Register the sentiment analysis function as a UDF (User Defined Function)
    val getSentimentUDF = udf(getSentiment _)

    // Apply sentiment analysis UDF to the DataFrame
    val dfWithSentiment = df.withColumn("predicted_sentiment", getSentimentUDF(col("text")))

    // Calculate the count of each sentiment
    val sentimentCounts = dfWithSentiment.groupBy("predicted_sentiment").count()

    // Calculate total number of records
    val totalCount = dfWithSentiment.count()

    // Calculate the percentage of each sentiment
    val sentimentPercentages = sentimentCounts.withColumn("percentage",
      col("count") / lit(totalCount) * 100)

    // Select the desired columns for output
    val dfToWrite = sentimentPercentages.select("predicted_sentiment", "percentage")

    // Show the DataFrame with selected columns
    dfToWrite.show(truncate = false)

    // Save the DataFrame with sentiment analysis results to a new CSV file using DataFrameWriter
    val percentageOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\sentiment_percentages.csv"

    // Write DataFrame to a single CSV file
    dfToWrite.write
      .mode("overwrite")  // Overwrite the file if it already exists
      .option("header", "true")  // Include column names as headers in the CSV file
      .csv(percentageOutputPath)

    // Split the DataFrame into separate DataFrames for each sentiment
    val positiveDF = dfWithSentiment.filter(col("predicted_sentiment") === "positive").na.drop()
    val negativeDF = dfWithSentiment.filter(col("predicted_sentiment") === "negative").na.drop()
    val neutralDF = dfWithSentiment.filter(col("predicted_sentiment") === "neutral").na.drop()









    // Define output paths for each sentiment
    val positiveOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\positive_reviews.csv"
    val negativeOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\negative_reviews.csv"
    val neutralOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\neutral_reviews.csv"

    // Write each sentiment DataFrame to separate CSV files
    positiveDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(positiveOutputPath)

    negativeDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(negativeOutputPath)

    neutralDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(neutralOutputPath)

    // Stop the Spark session
    spark.stop()
  }
}
