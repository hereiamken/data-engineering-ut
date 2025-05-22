package org.example.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.io.Source

object DataframeUtils {
  def createRandomDataFrameFromJSON(spark: SparkSession, jsonFilePath: String, numRows: Int): DataFrame = {
    // Read the JSON schema from file
    println(jsonFilePath)
    val jsonSchemaString = readSource(jsonFilePath)
    println(jsonSchemaString)

    // Parse the schema using Jackson
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val fields = mapper.readValue(jsonSchemaString, classOf[Array[Map[String, String]]])

    // Convert to StructType
    val structFields = fields.map { field =>
      val dataType = field("type").toLowerCase match {
        case "string" => StringType
        case "integer" | "int" => IntegerType
        case "long" => LongType
        case "double" => DoubleType
        case "boolean" => BooleanType
        case "date" => DateType
        case "timestamp" => TimestampType
        case _ => StringType
      }
      StructField(field("name"), dataType, nullable = true)
    }
    val schema = StructType(structFields)

    // Generate dummy data
    val rows = (1 to numRows).map { _ =>
      val values = schema.fields.map {
        case StructField(_, StringType, _, _) => java.util.UUID.randomUUID().toString
        case StructField(_, IntegerType, _, _) => scala.util.Random.nextInt(100)
        case StructField(_, LongType, _, _) => scala.util.Random.nextLong()
        case StructField(_, DoubleType, _, _) => scala.util.Random.nextDouble()
        case StructField(_, BooleanType, _, _) => scala.util.Random.nextBoolean()
        case StructField(_, DateType, _, _) => Date.valueOf(LocalDate.now().minusDays(scala.util.Random.nextInt(365)))
        case StructField(_, TimestampType, _, _) => Timestamp.valueOf(LocalDateTime.now().minusDays(scala.util.Random.nextInt(365)))
        case _ => null
      }
      Row(values: _*)
    }

    // Create DataFrame
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  /**
   * read from source
   *
   * @param path : string path
   * @return
   */
  private def readSource(path: String): String = {
    val jsonStr = Source.fromResource(path).getLines().mkString
    jsonStr
  }

  def createDataFrameFromJSON(spark: SparkSession, jsonFilePath: String, data: Seq[String]): DataFrame = {
    // Parse schema JSON file
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val jsonNode: Array[Map[String, String]] = mapper.readValue(Source.fromResource(jsonFilePath).getLines().mkString, classOf[Array[Map[String, String]]])

    // Map to StructType
    val fields = jsonNode.map { field =>
      val dataType = field("type").toLowerCase match {
        case "string" => StringType
        case "integer" => IntegerType
        case "int" => IntegerType
        case "long" => LongType
        case "double" => DoubleType
        case "boolean" => BooleanType
        case "date" => DateType
        case "timestamp" => TimestampType
        case _ => StringType
      }
      StructField(field("name"), dataType, nullable = true)
    }
    val schema = StructType(fields)

    // Convert string samples to Row
    val rows = data.map { line =>
      val tokens = line.split(",", -1).map(_.trim)
      val values = fields.zip(tokens).map { case (field, token) =>
        if (token.isEmpty) null
        else field.dataType match {
          case StringType => token
          case IntegerType => token.toInt
          case LongType => token.stripSuffix("L").toLong
          case DoubleType => token.toDouble
          case BooleanType => token.toBoolean
          case DateType => java.sql.Date.valueOf(token)
          case TimestampType => java.sql.Timestamp.valueOf(token)
          case _ => token
        }
      }
      Row(values: _*)
    }

    // Create DataFrame
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }
}

