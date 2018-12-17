package com.mf.data.until

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  def getMinutes():String={
    val now:Date = new Date()
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("mm")
    val minutes = dateFormat.format( now )
    minutes
  }
  def getNowTime():String={
    val now:Date = new Date()
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val Now = dateFormat.format( now )
    Now
  }
}
