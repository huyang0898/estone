package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object mongoDB_Demo {
  def main(args: Array[String]): Unit = {
    //1.初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val sc: SparkContext = ssc.sparkContext

    //3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams = ssc.socketTextStream("192.168.4.71", 9999)
//    lineStreams.foreachRDD(
//      s=>{
//        val fileRDD: RDD[String] = sc.textFile("input")
//        val value: RDD[String] = s.intersection(fileRDD)
//        value.collect().foreach(println)
//      }
//    )

    val valueStream: DStream[String] = lineStreams.transform(
      rdd => {
        val fileRDD: RDD[String] = sc.textFile("input")
        val value: RDD[String] = rdd.intersection(fileRDD,10)
        value

      }
    )
//    valueStream.foreachRDD(
//      rdd=>rdd.collect().foreach(println)
//    )

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }


}
