package com.mf.data

import com.mf.data.moxiePhone.MoxiePhone
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  * topic1,topic2
  */
object KafkaSreaming {
  val logg = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    if (args.length < 2) {
      logg.error("缺少配置参数")
      System.exit(1)
    }
    val Array(brokers, topics) = args
    // Create context with 10 second batch interval
    val conf: SparkConf = new SparkConf().setAppName("KafkaSreaming")
//          .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
    val sqc: SQLContext = new SQLContext(sc)

    try {
      MoxiePhone.MoxiePhone(sc, ssc, sqc, brokers, topics)
    } catch {
      case ex: Exception => logg.error("配置错误", ex)
    }
  }
}