package com.spark.bigdata.mlib

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object XgboostFeature {
  val trainPath=""
  val testPath=""
  val loadFormat="libsvm"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("XgboostFeature")
      .getOrCreate()

    val sc=spark.sparkContext

    val maxDepth = 3
    val numRound = 4
    val nworker = 1
    val paramMap = Seq(
      "eta" -> 0.1,
      "max_depth" -> maxDepth,
      "objective" -> "binary:logistic").toMap

    val dfTrain=MLUtils.loadLibSVMFile(sc,trainPath)
    val dfTest=MLUtils.loadLibSVMFile(sc,testPath)

    val xGBoostModel=XGBoost.train(dfTest,paramMap,numRound,nworker,useExternalMemory=false)

    val testData=dfTest.map(x=>{
      x.features
    })

    val result=xGBoostModel.predict(testData)

    result.foreach(println)

    spark.stop()




  }



}
