package com.mf.data.moxiePhone

import com.mf.data.until.DateUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MoxiePhone {
  val logg = Logger.getLogger(this.getClass)

  def MoxiePhone(sc: SparkContext, ssc: StreamingContext, sqc: SQLContext, brokers: String, topics: String): Unit = {
    //从dataframe中获取所需字段
    val blackPhone: DataFrame = MysqlBlackRadio.getMysql(sqc)
    //广播黑名单数据
    val blackBroadcast = sc.broadcast(blackPhone)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> "10485760"
      //      ,"auto.offset.reset" -> "smallest"
    )
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


      messages.foreachRDD { Rdd =>
        if (!Rdd.isEmpty()) {
          try {
          val rdd = Rdd.map(_._2)
          val sqc: SQLContext = SQLContext.getOrCreate(rdd.sparkContext)

          //整点更新广播变量
          if (DateUtils.getMinutes() == "00") {
            MysqlBlackRadio.update(rdd.sparkContext, sqc: SQLContext, true)
          }

          val d: DataFrame = sqc.jsonRDD(rdd)
          d.registerTempTable("moxiePhone")

          //获取广播变量
          val black: DataFrame = blackBroadcast.value
          //注册成临时表
          black.createOrReplaceTempView("blackPhone")

          //获取通话记录手机号
          val calls: DataFrame =
            sqc.sql(
              s"""
                 |SELECT mobile,explode(item.peer_number) peer_number
                 |from
                 |(select mobile,explode(calls.items) item
                 |from moxiePhone)
      """.stripMargin
            ).toDF()
          calls.registerTempTable("calls")

          //通话记录去重后的电话数
          val phoneNum: DataFrame =
            sqc.sql(
              s"""
                 |select distinct(peer_number) phoneNum,mobile from calls
      """.stripMargin
            ).toDF()
          phoneNum.registerTempTable("phoneNum")

          //通话记录中标次数 和 通话记录次数

          val call: DataFrame =
            sqc.sql(
              s"""
                 |select D.mobile ,D.callNum callNum ,IFNULL(C.callWin,0) callWin from
                 |(
                 |select mobile, count(A.peer_number) callWin  from
                 |calls A
                 |Left JOIN
                 |(select tel as phone from blackPhone) B
                 |ON
                 |A.peer_number = b.phone
                 |where b.phone is not null
                 |group by A.mobile
                 |) C
                 |right Join
                 |(
                 |select mobile ,count(peer_number) callNum from calls
                 |group by mobile
                 |)D
                 |ON C.mobile = D.mobile
      """.stripMargin
            )
          call.registerTempTable("call")

          //电话数中标次数
          val phone: DataFrame =
            sqc.sql(
              s"""
                 |select D.mobile ,D.phoneNum phoneNum,IFNULL(C.phoneWin,0) phoneWin from
                 |(
                 |select mobile, count(A.phoneNum) phoneWin  from
                 |phoneNum A
                 |Left JOIN
                 |(select tel as phone from blackPhone) B
                 |ON
                 |A.phoneNum = b.phone
                 |where b.phone is not null
                 |group by A.mobile
                 |) C
                 |right Join
                 |(
                 |select mobile ,count(phoneNum) phoneNum from phoneNum
                 |group by mobile
                 |)D
                 |ON C.mobile = D.mobile
      """.stripMargin
            )
          phone.registerTempTable("phone")

          //整合成一张表
          val result: DataFrame =
            sqc.sql(
              s"""
                 |select call.mobile userTel,call.callNum callNum,phone.phoneNum phoneNum
                 |,call.callWin callWin , phone.phoneWin phoneWin from
                 |call
                 |Left Join
                 |phone
                 |on call.mobile = phone.mobile
      """.stripMargin
            ).toDF()
          //结果打印到控制台，结果如果过大，不建议打印
          result.show()
          MysqlBlackRadio.writeMysql(result)
          } catch {
            case ex: Exception => logg.error("数据解析失败", ex)
        }
      }
    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}