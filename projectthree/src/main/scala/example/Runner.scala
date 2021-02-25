package example

import org.apache.spark.sql.SparkSession

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("local[4]")
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val s3DataMaybe = spark.read.text("s3a://usf-210104-big-data/twitterstream/tweetstream-1613536993819-1")
    s3DataMaybe.show()
  }
}