package com.spark.bigdata.kafka

import com.spark.bigdata.util.ConfigUtil
import net.ceedubs.ficus.readers.ArbitraryTypeReader._


object KafkaZKConfig {

  def getDefaultConf(configName: String = "kafkaZookeeper", rootNode: String = "config"): KafkaZKConfig = {
    import net.ceedubs.ficus.Ficus._
    val config = ConfigUtil.readClassPathConfig[ZkKafkaConf](configName, rootNode)
    config.buildConfig
  }
  case class KafkaZKConfig(zkList: String, brokerList: String)

  case class BasicConf(hosts: Seq[String], port: String)

  case class ZkKafkaConf(zk: BasicConf, kafka: BasicConf) {

    def buildConfig: KafkaZKConfig = {
      val z = zk.hosts.map(z => s"$z:${zk.port}").mkString(",")
      val k = kafka.hosts.map(k => s"$k:${kafka.port}").mkString(",")
      KafkaZKConfig(z, k)
    }

  }



}