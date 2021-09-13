package com.estone.esConnect

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.elasticsearch.spark._


object esMove {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf.setAppName("esrdd")
    conf.setMaster("local[*]")
    // 配置es配置信息
    conf.set("es.nodes", "10.100.1.71,10.100.1.72,10.100.1.73")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    conf.set("es.index.read.missing.as.empty", "true")


    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()


    import spark.implicits._


    val sc = spark.sparkContext


    val query =
      """
            {
      "query": {
        "match_all": {}
      }
    }
      """.stripMargin

    // 输出类型中，key=每条数据对应的id，value=id对应的数据. map中的key=字段名称
    //val sourceRDD: RDD[(String, String)] = sc.esJsonRDD("amazon_product_listing", query)
    val sourceRDD: RDD[(String, String)] = sc.esJsonRDD(args(0), query)



    val valueRDD: RDD[String] = sourceRDD.map(_._2)
    //valueRDD.saveAsTextFile("hdfs:hadoop101:8020/output")

    val value: RDD[String] = valueRDD.map(
      s => s.replace(
        "\\n", " "
      )
    )

    val value1: RDD[String] = value.map(
      s => s.replace(
        "\\r", " "
      )
    )


    val dataFrame: DataFrame = spark.read.option("multiLine", true)
      .option("mode","DROPMALFORMED")
      .json(value1)

    dataFrame.createOrReplaceGlobalTempView("estone")


    spark.sql("select count(*) from global_temp.estone").show()


    //写入到hive
    spark.sql("use gmall")
    spark.sql(args(1))



    sc.stop()


  }
}
