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
      .csv("D:\\SCALA\\test.csv")

    // Define a function to determine sentiment based on text
    def getSentiment(text: String): String = {
      // Implement your sentiment analysis logic here
      // Example: Use TextBlob or custom logic to determine sentiment
      // For simplicity, we'll assume a neutral sentiment for demonstration
      "neutral"
    }

    // Register the sentiment analysis function as a UDF (User Defined Function)
    val getSentimentUDF = udf(getSentiment _)

    // Apply sentiment analysis UDF to the DataFrame
    val dfWithSentiment = df.withColumn("predicted_sentiment", getSentimentUDF(col("text")))

    // Select the desired columns for output
    val dfToWrite: DataFrame = dfWithSentiment.select("text", "predicted_sentiment")

    // Show the DataFrame with selected columns
    dfToWrite.show(truncate = false)

    // Save the DataFrame with sentiment analysis results to a new CSV file using DataFrameWriter
    val outputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\test2.csv"

    // Write DataFrame to a single CSV file
    df.write
      .mode("overwrite")  // Overwrite the file if it already exists
      .option("header", "true")  // Include column names as headers in the CSV file
      .csv(outputPath)

    // Stop the Spark session
    spark.stop()
  }
}
