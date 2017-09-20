package com.spark.bigdata

import com.spark.bigdata.util.ConfigUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  */
package object es {
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  private lazy val config = ConfigUtil.readClassPathConfig[EsConf]("es", "config")

  implicit class EsConfig(conf: SparkSession.Builder) {

    def build(index: String, typeTable: String, mapId: String): SparkSession.Builder = {
      conf
        //        .config("spark.buffer.pageSize", "8m")
        .config("es.nodes", config.nodes)
        .config("es.port", config.port)
        .config("es.scroll.size", "2000")
        .config("es.resource", s"$index/$typeTable")
        .config("es.index.auto.create", "true")
        .config("es.write.operation", "upsert")
        .config("es.mapping.id", s"$mapId")
    }

  }

}

case class EsConf(nodes: String, port: String)



