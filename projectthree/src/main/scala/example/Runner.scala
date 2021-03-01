package example

import org.apache.spark.sql.SparkSession

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

    val df = spark.read.load("s3a://commoncrawl/cc-index/table/cc-main/warc/")

    df
    .select("url_host_name", "url_path")
    .filter($"crawl" === "CC-MAIN-2020-16")
    .filter($"subset" === "warc")
    .filter($"url_path".contains("job"))
    .show(200, false)
  }
}