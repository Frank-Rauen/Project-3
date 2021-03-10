package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.amazonaws.thirdparty.ion.Timestamp
import java.sql.Time

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("https://s3.console.aws.amazon.com/s3/buckets/project3databucket")
      .getOrCreate()

    val key = System.getenv("AWS_KEY_ID")
    val secret = System.getenv("AWS_SEC")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val columnarIndexDf =
    spark.read
      .format("csv")
      .option("header", "true")
      .load("s3://commoncrawl/cc-index/table/cc-main/warc/")
      .toDF("url_host_name", "url_path", "crawl", "fetch_time")
      
      
 columnarIndexDf
  .select(lower($"url_path").as("url"))
    .filter(($"url" like "%job%")&&
    (unix_timestamp($"fetch_time", "yyyy-mm-dd HH:mm:ss.S")
    .between(
      Timestamp.valueOf("2019-01-01 00:00:00"),
      Timestamp.valueOf("2019-01-31 23:59:59"),

    )) &&
    ($"crawl" like "CC-MAIN-2019-04")
      (($"url" like "%software%")or
      ($"url" like "%frontend%")or
      ($"url" like "%backend%")or
      ($"url" like "%fullstack%")or
      ($"url" like "%cybersecurity%")or
      ($"url" like "%computer%")or
      ($"url" like "%java%")or
      ($"url" like "%c++%")or
      ($"url" like "%data%")or
      ($"url" like "%web%developer%")or
      ($"url" like "%artificial%intelligence%")or
      ($"url" like "%network%")or
      ($"url" like "%programmer%")))
    .select(count($"url").as("total jobs"))
    .show()

}
}