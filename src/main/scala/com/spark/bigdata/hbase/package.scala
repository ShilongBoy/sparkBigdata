package com.spark.bigdata

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object hbase {

  val spark=SparkSession.builder().appName("").master("").getOrCreate()
  val sc=spark.sparkContext
  val conf= HBaseConfiguration.create()
  val habsecontext=new HBaseContext(sc,conf)


  def scanHbaseTB(tableName:String)(implicit startKey:Option[String],endKey:Option[String]):RDD[(ImmutableBytesWritable,Result)]={
  //如果有StartRowKey根据提供查询
    startKey match {
      case Some(x)=>{
        val scan=new Scan()
        scan.setStartRow(Bytes.toBytes(x))
        scan.setStopRow(Bytes.toBytes(endKey.getOrElse(x)))
        val hbaeRDD=habsecontext.hbaseRDD(TableName.valueOf(tableName),scan)
        hbaeRDD
      }
      case None=>{
        val scan=new Scan()
        val hbaeRDD=habsecontext.hbaseRDD(TableName.valueOf(tableName),scan)
        hbaeRDD
      }
    }


    def main(args: Array[String]): Unit = {

      val SparkHbaseRDD=scanHbaseTB("SparkHbase")
      SparkHbaseRDD.foreach(x=>{
        val rowKey=x._1.toString
        val rs=x._2
        val cell=rs.getColumnLatestCell(Bytes.toBytes(""),Bytes.toBytes(""))
        println(s"the rowKey is $rowKey the values is $cell")
      })


    }


  }

}
