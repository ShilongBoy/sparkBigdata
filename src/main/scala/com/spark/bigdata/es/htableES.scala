package com.spark.bigdata.es

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
/**
  *
  */
object htableES {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("SparkTest")
      .master("local[5]")
      .build("shopIndex","shop","shopId")
      .getOrCreate()

    import spark.implicits._
    val df=Seq(
      ("屈臣",Seq("武汉汉口北88号","江汉路店"),"08-12")

    ).toDF("shopName","shopAdress","shopTime")

    //写入ES
    df.saveToEs( spark.sqlContext.getAllConfs.toMap)
    spark.stop()

  }

  case class shopTable(shopName:String,shopAdress:Seq[String],shopTime:String)

}
