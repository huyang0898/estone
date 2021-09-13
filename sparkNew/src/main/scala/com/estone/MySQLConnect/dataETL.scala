package com.estone.MySQLConnect

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dataETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.100.1.122:3306/erp_product")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "readonly")
      .option("password", "")
      .option("dbtable", "user")
      .load().show

  }

}
