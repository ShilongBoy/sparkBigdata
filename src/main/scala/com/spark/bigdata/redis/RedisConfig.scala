package com.spark.bigdata.redis

import com.spark.bigdata.util.ConfigUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  */
object RedisConfig {

  lazy val redisConf=ConfigUtil.readClassPathConfig[RedisConf]("redis","sink")
  implicit class redisConf(sparkSession: SparkSession){
    def initRedis(): Unit ={
      sparkSession.conf.set("redis.host",redisConf.host)
      sparkSession.conf.set("redis.port",redisConf.host)
    }
  }



  case class RedisConf(host:String,port:Int)

}
