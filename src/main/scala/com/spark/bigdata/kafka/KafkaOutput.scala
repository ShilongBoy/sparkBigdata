package com.spark.bigdata.kafka

import com.spark.bigdata.util.FileReaderUtil
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.commons.codec.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
object KafkaOutput {

  val checkPotion="/opt/tmp/kafka/sparkCheckopt"
  val spark=SparkSession.builder().appName("").master("").getOrCreate()
  //val configuration=Configuration
  val brokers=FileReaderUtil.getConfig("","")

  def getOrCreateSc(): StreamingContext ={

    new StreamingContext(spark.sparkContext,Seconds(1))
  }

  def main(args: Array[String]): Unit = {

    //val ssc=StreamingContext.getOrCreate(checkPotion,()=>getOrCreateSc(),configuration)
    val topis=Set("testTopic")

    val mapPatis=Map( "metadata.broker.list" -> brokers)
    val conf = globalConf(args)
    conf.setMaster("local[*]")
    //val messageDstream=KafkaUtils.createDirectStream[StringDecoder,StringDecoder](ssc,)


  }


  def createSSC(args:Array[String],topicsSet: Set[String],zkNode:String,confMap: Map[String, String]):StreamingContext={
    val conf=globalConf(args)
    val ssc = new StreamingContext(conf, Seconds(args(1).toInt))
    ssc.checkpoint("")
    val fromOffset = TopicOffset(zkNode, confMap).getOffset(topicsSet)
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], String]) => (mmd.key, mmd.message)
    val messages = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder, (Array[Byte], String)](
      ssc, conf.getAll.toMap, fromOffset, messageHandler)

    messages.transform(rdd=>{
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      TopicOffset(zkNode,confMap).saveOffset(offsetRanges)
      rdd
    }).foreachRDD(x=>{
     x.foreach{
       case(as,ms)=>{
         println(s"the topic is $as and the ms is $ms")
       }
      }
    })



  ssc

  }

  def globalConf(args: Array[String]): SparkConf = {
    val conf = new SparkConf().setAppName("HotWordsJob-热词分析")
    //    conf.set("spark.hbase.host", zkQuoram)
    conf.set("spark.streaming.receiver.maxRate", "-1")
    conf.set("spark.streaming.blockInterval", "1s")
    conf.set("spark.streaming.backpressure.enabled", "true")
    //conf.set("spark.streaming.kafka.maxRatePerPartition", "100000")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[SparkConf], classOf[Configuration]))

    //hbase conf
    //    conf.set("hbase.zookeeper.quorum", zkQuoram)
    //    conf.set("hbase.rootdir", "/apps/hbase")
    //    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //    conf.set(TableOutputFormat.OUTPUT_TABLE, args(2))

    //kafka conf
    conf.set("group.id", "hotWordsJob")
    conf.set("zookeeper.connect", "")
    conf.set("client.id", "hotWords")
    conf.set("metadata.broker.list", "")
    conf.set("serializer.class", "kafka.serializer.StringEncoder")
    conf.set("key.serializer.class", "kafka.serializer.StringEncoder")
  }




}
