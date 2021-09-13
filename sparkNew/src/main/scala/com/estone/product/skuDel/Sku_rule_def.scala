package com.estone.product.skuDel



import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class Sku_rule_def {
  System.setProperty("HADOOP_USER_NAME", "root")
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sku_mv")
  val spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()


  import spark.implicits._


  def getResultRule1(s: Array[String]) = {


    val id = s(0)
    val order_status: String = s(4)
    val created_date_ago: Int = s(5).toInt
    val def_month_last: Int = s(6).toInt
    //val is_sale: String = s(7)
    val is_sale: Int = s(7).toInt
    val order_num_avg_def_min: Int = s(8).toInt
    val order_num_first_sign: String = s(9)
    val order_num_avg_def_max: Int = s(10).toInt
    val order_num_second_sign: String = s(11)
    val lastDate_months_ago: Int = s(12).toInt
    val gross_profit_avg_def_min: Double = s(13).toDouble
    val gross_profit_first_sign: String = s(14)
    val gross_profit_avg_def_max: Double = s(15).toDouble
    val gross_profit_second_sign: String = s(16)






    //第一步过滤订单状态：
    //
    spark.sql(
      s"""
         |select
         |id
         |from
         |(
         |select
         |id,
         |order_status,
         |row_number() over(partition by id order by modified_date desc) rk
         |from stg_customer_order
         |)t
         |where t.rk = 1 and order_status in ('$order_status')
         |""".stripMargin)
      .createOrReplaceGlobalTempView("filter_order_status_id")
    //第二步过滤掉指定录入时间的sku:
    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val do_date: String = format.format(date)
    spark.sql("use gmall")


    spark.sql(
      s"""
         |select
         |article_number,
         |launch_time
         |from dwd_dim_sku_info
         |where dt = '$do_date'
         |and launch_time < add_months('$do_date',-$created_date_ago)
         |""".stripMargin)
      .createOrReplaceGlobalTempView("filter_created_date_def")


    //第三步过滤掉指定日期的销量的sku:
    //获取是否有销量字段
    if (is_sale == 0) {


      //这种情况没有销量（不能考虑自定义订单状态，没有实际意义），可以理解为上次出单时间在一年之前
      spark.sql(
        s"""
           |select
           |article_number
           |from dwt_sku_order_date_topic
           |where order_date_last < add_months('$do_date',-$def_month_last)
           |""".stripMargin)
        .createOrReplaceGlobalTempView("filter_num_def_sku")
    }else if(is_sale == 1){
      //如果自定义时间段有销量，则月均销量满足大于最小自定义最小月均销量且小于自定义最大月均销量
      spark.sql(
        s"""
           |select
           |article_number,
           |sum(quantity)
           |from
           |(
           |select
           |article_number,
           |quantity
           |from (
           |select
           |customer_order_id,
           |article_number,
           |quantity,
           |row_number() over(partition by id order by modified_date desc) rk
           |from stg_customer_order_item
           |where dt >= add_months('$do_date',-$def_month_last)
           |)t
           |where rk = 1 and customer_order_id in(
           |select
           |id
           |from(
           |select
           |id,
           |order_status,
           |row_number() over(partition by id order by modified_date desc ) rn
           |from stg_customer_order
           |where dt >= add_months('$do_date',-$def_month_last)
           |)m
           |where rn = 1 and order_status in ('$order_status')
           |)
           |)tbl
           |group by article_number
           |having sum(quantity)/$def_month_last $order_num_first_sign $order_num_avg_def_min and sum(quantity)/$def_month_last $order_num_second_sign $order_num_avg_def_max
           |""".stripMargin)
        .createOrReplaceGlobalTempView("filter_num_def_sku")
    }


    //过滤掉最后一个订单往前推的自定义时间段，满足条件的sku
    spark.sql(
      s"""
         |select
         |article_number,
         |sum(grossprofit),
         |sum(quantity)
         |from(
         |select
         |t1.article_number,
         |t1.grossprofit,
         |t1.quantity,
         |t2.created_date,
         |t3.order_date_last
         |from(
         |select
         |customer_order_id,
         |article_number,
         |grossprofit,
         |quantity
         |from(
         |select
         |customer_order_id,
         |article_number,
         |grossprofit,
         |quantity,
         |row_number() over(partition by id order by modified_date desc ) rk
         |from stg_customer_order_item
         |)t
         |where rk = 1
         |)t1 join (
         |select
         |id,
         |created_date
         |from(
         |select
         |id,
         |created_date,
         |order_status,
         |row_number() over(partition by id order by modified_date desc) rn
         |from stg_customer_order
         |)m
         |where rn = 1 and order_status in ('$order_status')
         |)t2
         |on t1.customer_order_id = t2.id
         |join(
         |select * from dwt_sku_order_date_topic
         |)t3
         |on t1.article_number = t3.article_number
         |)tbl
         |where created_date <= order_date_last and created_date >= add_months(order_date_last,-$lastDate_months_ago)
         |group by article_number
         |having  sum(grossprofit)/sum(quantity) $gross_profit_first_sign $gross_profit_avg_def_min and sum(grossprofit)/sum(quantity) $gross_profit_second_sign $gross_profit_avg_def_max
         |""".stripMargin)
      .createOrReplaceGlobalTempView("lastDate_month_def_grossProfit")


    val ds: Dataset[sku_rm_result] = spark.sql(
      s"""
         |select
         |'$id' id,
         |k1.article_number
         |from global_temp.filter_created_date_def k1
         |join global_temp.filter_num_def_sku k2
         |on k1.article_number = k2.article_number
         |join global_temp.lastDate_month_def_grossProfit k3
         |on k1.article_number = k3.article_number
         |""".stripMargin).as[sku_rm_result]




    getResult(ds)


  }






  def getResultRule2(s: Array[String]) = {
    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val do_date: String = format.format(date)


    val id: Int = s(0).toInt
    val order_status: String = s(4)
    val created_date_ago: Int = s(5).toInt
    val def_month_last: Int = s(6).toInt
    val sales_put_freight_begin: Double = s(21).toDouble
    val sales_put_freight_begin_symbol: String = s(22)
    val sales_put_freight_end: Double = s(23).toDouble
    val sales_put_freight_end_symbol: String = s(24)


    //第一步过滤掉自定义录入时间的sku
    spark.sql(
      s"""
         |select
         |article_number
         |from dwd_dim_sku_info
         |where dt = '$do_date'
         |and launch_time < add_months('$do_date',-$created_date_ago)
         |""".stripMargin).createOrReplaceGlobalTempView("filter_created_date")


    //最近*月有销量的sku
    spark.sql(
      s"""
         |select
         |article_number
         |from dwt_sku_order_date_topic
         |where order_date_last > add_months('$do_date',-$def_month_last)
         |""".stripMargin).createOrReplaceGlobalTempView("filter_sale_having")


    //sku统计算法成本区间值统计


    spark.sql(
      s"""
         |select
         |k1.article_number,
         |k1.purchase_listing_cost,
         |k2.profit,
         |k2.weight_share_in_order
         |from(
         |select
         |article_number,
         |COUNT(DISTINCT purchaseOrderNo)*2.2 purchase_listing_cost
         |from(
         |select
         |t1.id,
         |t1.articleNumber article_number,
         |t1.purchaseOrderNo,
         |t2.purchaseDate,
         |t3.order_date_last
         |from (
         |select
         |id,
         |purchaseOrderNo,
         |articleNumber
         |from (
         |select
         |id,
         |articleNumber,
         |purchaseOrderNo,
         |row_number() over(partition by id order by modified_date desc) rk
         |from stg_purchase_order_item
         |)t
         |where rk = 1
         |)t1 join (
         |select
         |id,
         |purchaseDate,
         |purchaseOrderNo
         |from (
         |select
         |id,
         |purchaseDate,
         |purchaseOrderNo,
         |row_number() over(partition by id order by modified_date desc) rn
         |from stg_purchase_order
         |)m
         |where rn = 1
         |)t2
         |on t1.purchaseOrderNo = t2.purchaseOrderNo
         |join (
         |select
         |article_number,
         |order_date_last
         |from dwt_sku_order_date_topic
         |)t3
         |on t1.articleNumber = t3.article_number
         |)tbl
         |where purchaseDate <= order_date_last and purchaseDate >= add_months(order_date_last,-3)
         |group by article_number
         |)k1 join (
         |select
         |article_number,
         |sum(profit) profit,
         |sum(weight_share_in_order) weight_share_in_order
         |from (
         |select
         |id,
         |customer_order_id,
         |article_number,
         |profit,
         |weight_share_in_order
         |from(
         |select
         |id,
         |customer_order_id,
         |article_number,
         |profit,
         |weight_share_in_order,
         |row_number() over(partition by id order by order_create_time) rk
         |from stg_bonus_point
         |)t
         |where rk = 1 and customer_order_id in (
         |select
         |id
         |from (
         |select
         |id,
         |order_status,
         |row_number() over(partition by id order by modified_date) rn
         |from stg_customer_order where dt >= add_months('$do_date',-$def_month_last)
         |)m
         |where rn = 1 and order_status in $order_status
         |) and weight_share_in_order is not null
         |)tbl
         |group by article_number
         |)k2
         |on k1.article_number = k2.article_number
         |where $sales_put_freight_begin $sales_put_freight_begin_symbol (k2.profit - k2.weight_share_in_order - k1.purchase_listing_cost) $sales_put_freight_end_symbol $sales_put_freight_end
         |""".stripMargin).createOrReplaceGlobalTempView("filter_stats_cost")




    //最后汇总结果
    val ds: Dataset[sku_rm_result] = spark.sql(
      s"""
         |select
         |'$id' id,
         |p1.article_number
         |from global_temp.filter_created_date p1 join global_temp.filter_sale_having p2
         |on p1.article_number = p2.article_number
         |join global_temp.filter_stats_cost p3
         |on p1.article_number = p3.article_number
         |""".stripMargin).as[sku_rm_result]


    getResult(ds)




  }


  def getResultRule8(s: Array[String]) = {


    val date = new Date()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val do_date: String = format.format(date)


    val id: Int = s(0).toInt
    val order_status: String = s(4)
    val def_month_last: Int = s(6).toInt
    // val is_sale: String = s(7)
    val is_sale: Int = s(7).toInt
    val total_gross_begin: Double = s(38).toDouble
    val total_gross_begin_symbol: String = s(39)
    val total_gross_end: Double = s(40).toDouble
    val total_gross_end_symbol: String = s(41)
    val item_status: String = s(42)


    //第一步过滤掉单品状态
    spark.sql(
      s"""
         |select
         |article_number
         |from dwd_dim_sku_info
         |WHERE dt = '$do_date' and item_status = $item_status
         |""".stripMargin).createOrReplaceGlobalTempView("filter_item_status")


    //第二步自定义时间段里有没有 销量
    if (is_sale == 2) {
      spark.sql(
        s"""
           |select
           |article_number,
           |order_date_last
           |from dwt_sku_topic
           |where order_date_last < add_months('$do_date',-$def_month_last)
           |""".stripMargin).createOrReplaceGlobalTempView("filter_item_status")
    } else if (is_sale == 1) {
      spark.sql(
        s"""
           |select
           |article_number,
           |sum(grossprofit)
           |from (
           |select
           |article_number,
           |grossprofit
           |from (
           |select
           |customer_order_id,
           |article_number,
           |grossprofit,
           |row_number() over(partition by id order by modified_date desc) rk
           |from stg_customer_order_item
           |where dt >= add_months('$do_date',-$def_month_last)
           |)t
           |where rk = 1 and customer_order_id in(
           |select
           |id
           |from(
           |select
           |id,
           |order_status,
           |row_number() over(partition by id order by modified_date desc ) rn
           |from stg_customer_order
           |where dt >= add_months('$do_date',-$def_month_last)
           |)m
           |where rn = 1 and order_status in $order_status
           |group by art
           |having sum(grossprofit) $total_gross_begin_symbol $total_gross_begin and sum(grossprofit) $total_gross_end_symbol $total_gross_end
           |""".stripMargin).createOrReplaceGlobalTempView("filter_gross_profit")
    }


    val ds: Dataset[sku_rm_result] = spark.sql(
      s"""
         |select
         |$id id,
         |article_number sku
         |from global_temp.filter_gross_profit and article_number in (
         |select
         |article_number
         |from global_temp.filter_item_status
         |)
         |""".stripMargin).as[sku_rm_result]




    getResult(ds)




  }






  def getResult(ds: Dataset[sku_rm_result])= {
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/gmall_report")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "sku_rm_result")
      .mode(SaveMode.Append)
      .save()
  }

}
