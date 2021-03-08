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
      .master("https://s3.console.aws.amazon.com/s3/buckets/project3databucket")
      .getOrCreate()

    val key = System.getenv("AWS_KEY_ID")
    val secret = System.getenv("AWS_SEC")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration
    .set("fs.s3a.endpoint", "s3.amazonaws.com")

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")


    val columnarIndexDF =
    spark.read
      .format("csv")
      .option("header", "true")
      .load("s3://commoncrawl/cc-index/table/cc-main/warc/")
      
      
 
  spark.sql(s"""SELECT DISTINCT count(*) AS "Tech Jobs"
FROM 
    (SELECT url_path
    FROM "ccindex"."ccindex"
    TABLESAMPLE BERNOULLI(10) 
WHERE crawl LIKE 'CC-MAIN-2019-04'
AND subset = 'warc'
AND fetch_status = 200
AND content_languages = 'eng'
AND (fetch_time BETWEEN TIMESTAMP '2019-01-01 00:00:00' AND TIMESTAMP '2019-01-31 23:59:59')
  AND (LOWER(url_path) LIKE '%job%') AND 
      (LOWER(url_path) LIKE '%frontend%' OR
      LOWER(url_path) LIKE '%backend%' OR
      LOWER(url_path) LIKE '%fullstack%' OR
      LOWER(url_path) LIKE '%cybersecurity%' OR
      LOWER(url_path) LIKE '%software%' OR
      LOWER(url_path) LIKE '%computer%' OR
      LOWER(url_path) LIKE '%python%' OR
      LOWER(url_path) LIKE '%java%' OR
      LOWER(url_path) LIKE '%c++%' OR
      LOWER(url_path) LIKE '%data%scientist%' OR 
      LOWER(url_path) LIKE '%web%developer%' OR 
      LOWER(url_path) LIKE '%artificial%intelligence%' OR
      LOWER(url_path) LIKE '%network%' OR 
      LOWER(url_path) LIKE '%programmer%'))""").show()



}
}