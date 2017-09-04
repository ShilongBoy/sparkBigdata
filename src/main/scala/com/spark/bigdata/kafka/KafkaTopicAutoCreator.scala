package com.spark.bigdata.kafka

import kafka.admin.AdminUtils
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}


object KafkaTopicAutoCreator {

  private lazy val zkConnection=new ZkConnection("localhost:2181",1000)
//  private lazy val zkUtils=new ZkUtils(zkClient,zkConnection,true)
  private lazy val zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer)
  private lazy val partitioner = 3
  private lazy val replicationFactor = 1

  def createTopics(topics: Set[String]) = {

    topics.foreach(topic => {
      if (!AdminUtils.topicExists(zkClient, topic)) {
        AdminUtils.createTopic(zkClient, topic, partitioner, replicationFactor)
      }
    })
  }

}
