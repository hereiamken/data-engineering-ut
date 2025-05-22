package org.example.config

import org.apache.spark.SparkContext

object SparkContextCommon {
  lazy val sparkContext: SparkContext = {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkTest")
    new SparkContext(conf)
  }

  def stop(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
    }
  }
}
