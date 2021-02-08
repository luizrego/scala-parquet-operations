package org.lrdatalab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main extends App {
  //define scala session
  val spark: SparkSession = SparkSession.builder()
  .master("local[2]")
  .appName("LoadParquetV1")
  .getOrCreate()

  // read parquet file
  val df = spark.read.parquet("src/main/resources/input/expedia_train.parquet").select(col("year"),col("month"),col("date_time"),col("site_name"),col("user_id")).filter(col("year") === 2014).filter(col("month") === 1)
  // write effectively to the parquet file using repartition.

  df.repartition(30)
    .write
    .partitionBy("year","month")
    .parquet("src/main/resources/output/expedia_train_bucket_2014_01.parquet")
}