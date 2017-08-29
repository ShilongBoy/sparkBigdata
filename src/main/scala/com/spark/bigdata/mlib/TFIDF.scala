package com.spark.bigdata.mlib
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object TFIDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TFIDF").master("local[*]").getOrCreate()

    val df = spark.createDataFrame(
      Seq(
        ("1.0", "Hi i love spark"),
        ("0.0", "i think java is the best"),
        ("1.0", "Logistic regression models are neat")
      )).toDF("lable", "senstence")

    val tokenizer = new Tokenizer().setInputCol("senstence").setOutputCol("words")
    val wordsData = tokenizer.transform(df)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featureData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featureData)

    val rescaledData = idfModel.transform(featureData)
    rescaledData.select("lable", "features").show()

    spark.stop()

  }
}
