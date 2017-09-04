package com.spark.bigdata.util

import com.alibaba.fastjson.JSON
/**
  * Created by Sean on
  */
object JsonUtil {
  //jackson方式


  def readString2Bean[T](jsonString:String,clazz: Class[T]){

      JSON.parseObject[T](jsonString,clazz)
  }

  def main(args: Array[String]): Unit = {



  }

  case class  Tea(tname:String,tage:Int,tadd:Seq[String])

}
