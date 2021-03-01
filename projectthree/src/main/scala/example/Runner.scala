package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * This script reads 500k rows of data that I pulled in from the CC columnar index using EMR
  * This should be a manageable size to practice running aggregations with the CC index
  * 
  * I used the following filters when pulling this data in from EMR:
  *   val crawl = "CC-MAIN-2020-05"
  *   val jobUrls = df
  *    .select("url_host_name", "url_path")
  *    .filter($"crawl" === crawl)
  *    .filter($"subset" === "warc" )
  *    .filter($"url_host_registry_suffix" === "com")
  *    .filter($"url_path".contains("job"))
  **/

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("local[*]")
      .getOrCreate()

    val key = System.getenv("AWS_KEY_ID")
    val secret = System.getenv("AWS_SEC")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // I'm 98% sure I have this bucket configured so that everyone can access it
    // If you get a 403 error, let me know. It just means I need to tweak the bucket premissions further
    val s3bucket = "s3a://emr-output-revusf/joburls_CC-MAIN-2020-05_500k/"

    val jobUrls = spark.read
      .format("csv")
      .option("header", "false")
      .load(s3bucket)
      .toDF("url_host_name", "url_path")

    // This query takes a minute or two to run
    jobUrls
      .select("url_host_name")
      .groupBy("url_host_name")
      .agg(count("url_host_name"))
      .sort(desc("count(url_host_name)"))
      .show(10, false)
  }
}