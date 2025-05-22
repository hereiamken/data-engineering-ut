package org.example.jobs

import org.apache.spark.sql.SparkSession
import org.example.common.DataframeUtils.{createDataFrameFromJSON, createRandomDataFrameFromJSON}

object AppTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    println(s"Spark: ${spark.version}")

    val df1 = createRandomDataFrameFromJSON(spark = spark, jsonFilePath = "jobs/input_schema.json", numRows = 5)
    df1.show(false)

    val datas = Seq(
      "ID_1,Haiphong,2024-10-30 15:26:42.995598",
      "ID_2,Hanoi,2025-05-20 15:26:42.995598"
    )
    val df2 = createDataFrameFromJSON(spark = spark, jsonFilePath = "jobs/input_schema.json", data = datas)
    df2.show(false)

    spark.stop()
  }
}
