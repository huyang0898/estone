package com.estone.amazon.sellerSkuStats.bean

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

object orderCountZero {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf.setAppName("esrdd")
    conf.setMaster("local[*]")
    // 配置es配置信息
    conf.set("es.nodes","10.100.1.71,10.100.1.72,10.100.1.73")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create","true")
    conf.set("es.index.read.missing.as.empty", "true")


    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    val sc = spark.sparkContext

    spark.sql("use gmall")

    val dataFrame: DataFrame = spark.sql(
      """
        |select
        |id,
        |order_24H_count,
        |order_last_7d_count,
        |order_last_14d_count,
        |order_last_30d_count
        |from stg_amazon_product_listing_zero
        |""".stripMargin)

    //val dataFrame: DataFrame = spark.sql(args(0))

    val config = scala.collection.mutable.Map("es.resource.write" -> "amazon_product_listing/esAmazonProductListing",
      "es.mapping.id"->"id",
      "es.write.operation"->"upsert")

    dataFrame.saveToEs(config)

    spark.stop()
  }
}
