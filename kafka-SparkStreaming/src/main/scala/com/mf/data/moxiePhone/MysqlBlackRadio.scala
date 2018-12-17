package com.mf.data.moxiePhone

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext}


object MysqlBlackRadio {
  @volatile private var instance: Broadcast[DataFrame] = null
  val pro = new Properties()
  val ins: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("ConfigInfo.properties")
  pro.load(ins)

  def getMysql(sqc: SQLContext): DataFrame = {
    //添加配置
    val url = pro.getProperty("url")
    val username = pro.getProperty("username")
    val password = pro.getProperty("password")
    val table = pro.getProperty("table")
    val column = pro.getProperty("column")
    ins.close();

    //创建jdbc连接信息
    val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", username)
    prop.put("password", password)

    //加载mysql数据表
    val df_test1: DataFrame = sqc.read.jdbc(url, table, prop)
    //从dataframe中获取所需字段
    val blackPhone: DataFrame = df_test1.select(column)
    blackPhone
  }

  def writeMysql(DF: DataFrame): Unit = {
    val url = pro.getProperty("wurl")
    val username = pro.getProperty("wusername")
    val password = pro.getProperty("wpassword")
    val table = pro.getProperty("wtable")
    val properties = new Properties()
    properties.setProperty("user", username)
    properties.setProperty("password", password)
    DF.write.mode("append").jdbc(url, table, properties)
  }


  def update(sc: SparkContext, sqc: SQLContext, blocking: Boolean = false): Unit = {
    if (instance != null)
      instance.unpersist(blocking)
    instance = sc.broadcast(getMysql(sqc: SQLContext))
  }

  def getInstance(sc: SparkContext, sqc: SQLContext): Broadcast[DataFrame] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(getMysql(sqc: SQLContext))
        }
      }
    }
    instance
  }
}
