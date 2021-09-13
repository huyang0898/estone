package com.estone.product.skuDel

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Sku_rule_del_app {
  def main(args: Array[String]): Unit = {
    // val do_date = args(0)
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sku_mv")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()


    import spark.implicits._


    spark.sql("use gmall")


    //    val array1: Array[String] = spark.sql(
    //      """
    //        |select
    //        |*
    //        |from stg_sku_automatic_out2
    //        |""".stripMargin).collect()
    //      .map(
    //        row => row.mkString(" ")
    //      )




    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "admin0808")


    val df: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.4.87:3306/erp_product", "sku_automatic_out", props)
    df.createOrReplaceGlobalTempView("estone")


    val array: Array[String] = spark.sql(
      s"""
         |select
         |*
         |from global_temp.estone
         |where substring(create_at,1,10) = '2021-09-01' or substring(update_at,1,10) = '2021-09-01'
        """.stripMargin).collect()
      .map(
        row => row.mkString(" ")
      )
    val sku_rule_def = new Sku_rule_def
    for (i <- 0 until array.length ){
      val s: Array[String] = array(i).split(" ")
      if (s(1).toInt == 1){
        println("调用规则一")
        sku_rule_def.getResultRule1(s)
      }else if(s(1).toInt == 2){
        println("调用规则二")
        //sku_rule_def.getResultRule2(s)
      }else if(s(1).toInt == 8){
        println("调用规则八")
        // sku_rule_def.getResultRule8(s)
      }else{
        println("非规则一，二，八，不做需处理")
      }
    }
  }



}
