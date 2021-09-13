package com.estone.esConnect

import com.estone.amazon.sellerSkuStats.util.MyESUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL


object SparkToES {
  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "root")


    val conf = new SparkConf()
    // conf.setJars(Array("out/artifacts/spark_jar/spark.jar"))
    conf.setAppName("esrdd")
    conf.setMaster("local[*]")
    // 配置es配置信息
    conf.set("es.nodes","192.168.4.71,192.168.4.72,192.168.4.73")
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
    val dataFrame: DataFrame = spark.sql("select * FROM stg_amazon_product_listing_test")









//    value.foreachPartition(
//      it=>MyESUtil.insertBulk("amazon_product_listing_test",it)
//    )
  }


}


