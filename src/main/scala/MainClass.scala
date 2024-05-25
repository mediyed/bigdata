import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MainClass {
  def main(args: Array[String]): Unit = {
     val spark = SparkSession.builder()
      .appName("Sentiment Analysis with Scala and Spark")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .master("local[*]")
      .getOrCreate()

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

     val df = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\dataset.csv")

     def getSentiment(text: String): String = {
       if (text == null) "neutral"
      else {
        if (text.contains("good") || text.contains("happy") || text.contains("great")) "positive"
        else if (text.contains("bad") || text.contains("sad") || text.contains("terrible")) "negative"
        else "neutral"
      }
    }

     val getSentimentUDF = udf(getSentiment _)

     val dfWithSentiment = df.withColumn("predicted_sentiment", getSentimentUDF(col("text")))

     val sentimentCounts = dfWithSentiment.groupBy("predicted_sentiment").count()

     val totalCount = dfWithSentiment.count()

     val sentimentPercentages = sentimentCounts.withColumn("percentage",
      col("count") / lit(totalCount) * 100)

     val dfToWrite = sentimentPercentages.select("predicted_sentiment", "percentage")

     dfToWrite.show(truncate = false)

     val percentageOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\sentiment_percentages.csv"

     dfToWrite.write
      .mode("overwrite")
      .option("header", "true")
      .csv(percentageOutputPath)

     val positiveDF = dfWithSentiment.filter(col("predicted_sentiment") === "positive").na.drop()
    val negativeDF = dfWithSentiment.filter(col("predicted_sentiment") === "negative").na.drop()
    val neutralDF = dfWithSentiment.filter(col("predicted_sentiment") === "neutral").na.drop()



    val positiveOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\positive_reviews.csv"
    val negativeOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\negative_reviews.csv"
    val neutralOutputPath = "C:\\Users\\mohamediyed.ziri\\Documents\\bigdata\\neutral_reviews.csv"

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

    spark.stop()
  }
}
