package com.spark.bigdata.kafka

import com.spark.bigdata.util.FileReaderUtil
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.commons.codec.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import com.spark.bigdata.redis.RedisConfig._
import com.redislabs.provider.redis._
import com.spark.bigdata.util.JsonUtil._
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

  }

  def createSSC(args:Array[String],topicsSet: Set[String],zkNode:String,confMap: Map[String, String]):StreamingContext={
    val conf=globalConf(args)
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(args(1).toInt))
    ssc.checkpoint("")
    val fromOffset = TopicOffset(zkNode, confMap).getOffset(topicsSet)
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], String]) => (mmd.key, mmd.message)
    val messages = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder, (Array[Byte], String)](
      ssc, conf.getAll.toMap, fromOffset, messageHandler)

    val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())
    val mappingFunc = (word: String, count: Option[Int], state: State[Int]) => {
      val sum = count.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val msgRDD=messages.transform(rdd=>{
      //读取RDD的offset并保存
      val offsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      TopicOffset(zkNode,confMap).saveOffset(offsetRanges)
      rdd
    }).mapPartitions(x=>{
     x.map{
       case(as,ms)=>{
         val fakePolicy=readAsBeanByJson4s[FakePolicy](ms)
         val emid=fakePolicy.emid
         val clikCount=fakePolicy.clikCount
         (emid,clikCount.toInt)
       }
      }
    })

    //全局状态进行相加
    val upStateMSRDD=msgRDD.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    //Todo把解析的数据存储到Redis
      upStateMSRDD.foreachRDD(x=>{
        val saveRedisRDD=x.map(y=>(y._1,y._2.toString))
      sc.toRedisKV(saveRedisRDD)
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
    <!-- redis-->
    conf.set("redis.host","localhost")
    conf.set("redis.prot","6379")
  }



  case class FakePolicy(emid:String,clikCount:String)


}
