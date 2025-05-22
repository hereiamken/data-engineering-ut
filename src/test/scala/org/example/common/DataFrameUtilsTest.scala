package org.example.common

import org.apache.spark.sql.Row
import org.example.base.SparkTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

class DataFrameUtilsTest extends AnyFlatSpec with Matchers with SparkTestBase with MockitoSugar {

  "DataframeUtils.createRandomDataFrameFromJSON" should "create a DataFrame with the correct schema and number of rows" in {
    logger.info("Testing createRandomDataFrameFromJSON with simple schema")
    val schemaPath = "schemas/simple_schema.json"
    val numRows = 5
    val df = DataframeUtils.createRandomDataFrameFromJSON(spark, schemaPath, numRows)

    // Verify schema
    df.schema.fieldNames should contain theSameElementsAs Seq("id", "name")
    df.schema("id").dataType shouldBe org.apache.spark.sql.types.IntegerType
    df.schema("name").dataType shouldBe org.apache.spark.sql.types.StringType

    // Verify number of rows
    df.count() shouldBe numRows

    // Verify data types in generated data (basic check for non-null and correct type)
    df.collect().foreach { row =>
      row.get(0) shouldBe a[java.lang.Integer] // id
      row.get(1) shouldBe a[java.lang.String] // name
    }
  }

  it should "create a DataFrame with all supported random data types" in {
    logger.info("Testing createRandomDataFrameFromJSON with all_types_schema")
    val schemaPath = "schemas/all_types_schema.json"
    val numRows = 3
    val df = DataframeUtils.createRandomDataFrameFromJSON(spark, schemaPath, numRows)

    // Verify schema
    df.schema.fieldNames should contain theSameElementsAs Seq(
      "stringCol", "intCol", "longCol", "doubleCol", "booleanCol", "dateCol", "timestampCol"
    )
    df.schema("stringCol").dataType shouldBe org.apache.spark.sql.types.StringType
    df.schema("intCol").dataType shouldBe org.apache.spark.sql.types.IntegerType
    df.schema("longCol").dataType shouldBe org.apache.spark.sql.types.LongType
    df.schema("doubleCol").dataType shouldBe org.apache.spark.sql.types.DoubleType
    df.schema("booleanCol").dataType shouldBe org.apache.spark.sql.types.BooleanType
    df.schema("dateCol").dataType shouldBe org.apache.spark.sql.types.DateType
    df.schema("timestampCol").dataType shouldBe org.apache.spark.sql.types.TimestampType

    // Verify number of rows
    df.count() shouldBe numRows

    // Verify data types in generated data
    df.collect().foreach { row =>
      row.get(0) shouldBe a[java.lang.String] // stringCol
      row.get(1) shouldBe a[java.lang.Integer] // intCol
      row.get(2) shouldBe a[java.lang.Long] // longCol
      row.get(3) shouldBe a[java.lang.Double] // doubleCol
      row.get(4) shouldBe a[java.lang.Boolean] // booleanCol
      row.get(5) shouldBe a[java.sql.Date] // dateCol
      row.get(6) shouldBe a[java.sql.Timestamp] // timestampCol
    }
  }

  "DataframeUtils.createDataFrameFromJSON" should "create a DataFrame with correct schema and provided data" in {
    logger.info("Testing createDataFrameFromJSON with simple schema and sample data")
    val schemaPath = "schemas/simple_schema.json"
    val sampleData = Seq(
      "1,Alice",
      "2,Bob",
      "3,Charlie"
    )
    val df = DataframeUtils.createDataFrameFromJSON(spark, schemaPath, sampleData)

    // Verify schema
    df.schema.fieldNames should contain theSameElementsAs Seq("id", "name")
    df.schema("id").dataType shouldBe org.apache.spark.sql.types.IntegerType
    df.schema("name").dataType shouldBe org.apache.spark.sql.types.StringType

    // Verify content
    val expectedData = Seq(
      Row(1, "Alice"),
      Row(2, "Bob"),
      Row(3, "Charlie")
    )
    df.collect() should contain theSameElementsAs expectedData
  }

  it should "handle all supported data types and null values correctly" in {
    logger.info("Testing createDataFrameFromJSON with all_types_schema and diverse data")
    val currentDate = Date.valueOf(LocalDate.now())
    val currentTime = Timestamp.valueOf(LocalDateTime.now())
    val schemaPath = "schemas/all_types_schema.json"
    val sampleData = Seq(
      s"str1,10,100L,1.1,true,${currentDate},${currentTime}",
      "str2,20,200L,2.2,false,,", // Null date and timestamp
      "str3,30,300L,3.3,true,2023-01-01,2023-01-01 12:00:00.0"
    )
    val df = DataframeUtils.createDataFrameFromJSON(spark, schemaPath, sampleData)

    // Verify schema (already done in previous test, but good to re-check)
    df.schema("stringCol").dataType shouldBe org.apache.spark.sql.types.StringType
    df.schema("intCol").dataType shouldBe org.apache.spark.sql.types.IntegerType
    df.schema("longCol").dataType shouldBe org.apache.spark.sql.types.LongType
    df.schema("doubleCol").dataType shouldBe org.apache.spark.sql.types.DoubleType
    df.schema("booleanCol").dataType shouldBe org.apache.spark.sql.types.BooleanType
    df.schema("dateCol").dataType shouldBe org.apache.spark.sql.types.DateType
    df.schema("timestampCol").dataType shouldBe org.apache.spark.sql.types.TimestampType

    // Verify content including nulls and specific values
    val rows = df.collect()
    rows.length shouldBe 3

    rows(0).getString(0) shouldBe "str1"
    rows(0).getInt(1) shouldBe 10
    rows(0).getLong(2) shouldBe 100
    rows(0).getDouble(3) shouldBe 1.1 +- 0.001 // Use +- for Double comparison
    rows(0).getBoolean(4) shouldBe true
    rows(0).getDate(5) shouldBe currentDate
    rows(0).getTimestamp(6) shouldBe currentTime

    rows(1).getString(0) shouldBe "str2"
    rows(1).getInt(1) shouldBe 20
    rows(1).getLong(2) shouldBe 200
    rows(1).getDouble(3) shouldBe 2.2 +- 0.001
    rows(1).getBoolean(4) shouldBe false
    rows(1).isNullAt(5) shouldBe true // Check for null Date using isNullAt
    rows(1).isNullAt(6) shouldBe true // Check for null Timestamp using isNullAt

    rows(2).getString(0) shouldBe "str3"
    rows(2).getInt(1) shouldBe 30
    rows(2).getLong(2) shouldBe 300L
    rows(2).getDouble(3) shouldBe 3.3 +- 0.001
    rows(2).getBoolean(4) shouldBe true
    rows(2).getDate(5) shouldBe Date.valueOf("2023-01-01")
    rows(2).getTimestamp(6) shouldBe Timestamp.valueOf("2023-01-01 12:00:00.0")
  }
}
