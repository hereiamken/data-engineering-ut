package org.example.common

import org.apache.spark.sql.types._
import org.example.base.SparkTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Path
import scala.collection.JavaConverters.asScalaIteratorConverter

class DatabaseUtilsTest extends AnyFlatSpec with Matchers with SparkTestBase with MockitoSugar {
  "getConnectProps" should "return a map with correct connection properties" in {
    val username = "testuser"
    val password = "testpassword"
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/mydb"

    val result = DatabaseUtils.getConnectProps(username, password, driver, url)

    result shouldBe a[Map[_, _]]
    result should have size 4
    result.keys should contain theSameElementsAs Seq("user", "password", "driver", "url")
    result("user") shouldBe username
    result("password") shouldBe password
    result("driver") shouldBe driver
    result("url") shouldBe url
  }

  it should "handle empty string inputs correctly" in {
    val username = ""
    val password = ""
    val driver = ""
    val url = ""

    val result = DatabaseUtils.getConnectProps(username, password, driver, url)

    result should have size 4
    result("user") shouldBe ""
    result("password") shouldBe ""
    result("driver") shouldBe ""
    result("url") shouldBe ""
  }

  it should "handle special characters in inputs" in {
    val username = "user@123"
    val password = "p@ssw0rd!"
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://host:5432/db?ssl=true"

    val result = DatabaseUtils.getConnectProps(username, password, driver, url)

    result("user") shouldBe username
    result("password") shouldBe password
    result("driver") shouldBe driver
    result("url") shouldBe url
  }

  "getDBTypeByDriverClass" should "correctly map known JDBC driver classes to DB types" in {
    DatabaseUtils.getDBTypeByDriverClass("oracle.jdbc.driver.OracleDriver") shouldBe "ORACLE"
    DatabaseUtils.getDBTypeByDriverClass("oracle.jdbc.OracleDriver") shouldBe "ORACLE"
    DatabaseUtils.getDBTypeByDriverClass("org.postgresql.Driver") shouldBe "POSTGRES"
    DatabaseUtils.getDBTypeByDriverClass("com.mysql.jdbc.Driver") shouldBe "MYSQL"
    DatabaseUtils.getDBTypeByDriverClass("com.mysql.cj.jdbc.Driver") shouldBe "MYSQL"
    DatabaseUtils.getDBTypeByDriverClass("com.microsoft.sqlserver.jdbc.SQLServerDriver") shouldBe "SQLSERVER"
    DatabaseUtils.getDBTypeByDriverClass("org.mariadb.jdbc.Driver") shouldBe "MARIADB"
    DatabaseUtils.getDBTypeByDriverClass("org.apache.hive.jdbc.HiveDriver") shouldBe "HIVE"
    DatabaseUtils.getDBTypeByDriverClass("org.apache.hive.jdbc.HiveDriver2") shouldBe "HIVE2"
    DatabaseUtils.getDBTypeByDriverClass("org.sqlite.JDBC") shouldBe "SQLITE"
    DatabaseUtils.getDBTypeByDriverClass("com.snowflake.client.jdbc.SnowflakeDriver") shouldBe "SNOWFLAKE"
    DatabaseUtils.getDBTypeByDriverClass("org.h2.Driver") shouldBe "H2"
  }

  it should "return 'UNKNOWN' for unsupported or unrecognized driver classes" in {
    DatabaseUtils.getDBTypeByDriverClass("com.unsupported.Driver") shouldBe "UNKNOWN"
    DatabaseUtils.getDBTypeByDriverClass("another.random.Driver") shouldBe "UNKNOWN"
    DatabaseUtils.getDBTypeByDriverClass("") shouldBe "UNKNOWN"
    DatabaseUtils.getDBTypeByDriverClass(null) shouldBe "UNKNOWN" // Test with null input
  }

  "getDriverClassByDBType" should "correctly map known DB types to JDBC driver classes" in {
    DatabaseUtils.getDriverClassByDBType("ORACLE") shouldBe "oracle.jdbc.OracleDriver"
    DatabaseUtils.getDriverClassByDBType("POSTGRES") shouldBe "org.postgresql.Driver"
    DatabaseUtils.getDriverClassByDBType("MYSQL") shouldBe "com.mysql.cj.jdbc.Driver"
    DatabaseUtils.getDriverClassByDBType("SQLSERVER") shouldBe "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    DatabaseUtils.getDriverClassByDBType("MARIADB") shouldBe "org.mariadb.jdbc.Driver"
    DatabaseUtils.getDriverClassByDBType("HIVE") shouldBe "org.apache.hive.jdbc.HiveDriver"
    DatabaseUtils.getDriverClassByDBType("HIVE2") shouldBe "org.apache.hive.jdbc.HiveDriver2"
    DatabaseUtils.getDriverClassByDBType("SQLITE") shouldBe "org.sqlite.JDBC"
    DatabaseUtils.getDriverClassByDBType("SNOWFLAKE") shouldBe "com.snowflake.client.jdbc.SnowflakeDriver"
    DatabaseUtils.getDriverClassByDBType("H2") shouldBe "org.h2.Driver"
  }

  it should "handle case-insensitivity for DB types" in {
    DatabaseUtils.getDriverClassByDBType("oracle") shouldBe "oracle.jdbc.OracleDriver"
    DatabaseUtils.getDriverClassByDBType("postgres") shouldBe "org.postgresql.Driver"
    DatabaseUtils.getDriverClassByDBType("mysql") shouldBe "com.mysql.cj.jdbc.Driver"
    DatabaseUtils.getDriverClassByDBType("SQLSERVER") shouldBe "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }

  it should "throw IllegalArgumentException for unsupported DB types" in {
    a[IllegalArgumentException] should be thrownBy {
      DatabaseUtils.getDriverClassByDBType("UNKNOWN_DB")
    }
    a[IllegalArgumentException] should be thrownBy {
      DatabaseUtils.getDriverClassByDBType("CASSANDRA")
    }
    a[IllegalArgumentException] should be thrownBy {
      DatabaseUtils.getDriverClassByDBType("")
    }
    a[NullPointerException] should be thrownBy {
      DatabaseUtils.getDriverClassByDBType(null) // Test with null input
    }
  }

//  "DataframeUtils.readDB" should "correctly read data from a JDBC source" in {
//    val sampleData = Seq(
//      "1,Alice",
//      "2,Bob",
//      "3,Charlie"
//    )
//
//    val schema = StructType(List(
//      StructField("id", IntegerType, nullable = true),
//      StructField("name", StringType, nullable = true)
//    ))
//
//    val data = DataframeUtils.createDataFrameFromJSON(spark, "schemas/simple_schema.json", sampleData)
//    val tempPath = java.nio.file.Files.createTempDirectory("jdbcTest").toUri.toString
//    data.write.mode("overwrite").parquet(tempPath)
//
//    val mockJdbcOptions = Map(
//      "url" -> "jdbc:dummy",
//      "driver" -> "org.apache.hive.jdbc.HiveDriver",
//      "query" -> "SELECT * FROM mock",
//      "path" -> tempPath
//    )
//
//    // Spark doesn't support mocking JDBC reads directly without a real connection,
//    // so we simulate by reading from the path manually to compare.
//    val resultDF = DatabaseUtils.readDB(spark, "SELECT * FROM mock", mockJdbcOptions)
//
//    val simulatedDF = spark.read.schema(schema).parquet(tempPath)
//
//    resultDF.collect() should contain theSameElementsAs simulatedDF.collect()
//  }

  "readParquet" should "read back a DataFrame with or without schema" in {
    deleteDir("src/test/data")
    val schemaPath = "schemas/simple_schema.json"
    val sampleData = Seq(
      "1,Alice",
      "2,Bob",
      "3,Charlie"
    )
    val df = DataframeUtils.createDataFrameFromJSON(spark, schemaPath, sampleData)
    df.write.parquet("src/test/data")

    val readDfWithoutSchema = DatabaseUtils.readParquet(spark, "src/test/data")
    readDfWithoutSchema.count() shouldBe 3

    val schema = new StructType().add("id", "int").add("name", "string")
    val readDfWithSchema = DatabaseUtils.readParquet(spark, "src/test/data", schema)
    readDfWithSchema.schema shouldBe schema

    deleteDir("src/test/data")
  }

  "writeParquet" should "write a DataFrame with optional partitions" in {
    deleteDir("src/test/data")
    val schemaPath = "schemas/simple_schema.json"
    val sampleData = Seq(
      "1,Alice",
      "2,Bob",
      "3,Charlie"
    )
    val df = DataframeUtils.createDataFrameFromJSON(spark, schemaPath, sampleData)

    DatabaseUtils.writeParquet("src/test/data", df, numPartitions = Some(1))

    val files = new java.io.File("src/test/data").listFiles().filter(_.getName.endsWith(".parquet"))
    files.length should be > 0

    deleteDir("src/test/data")
  }

  def deleteDir(path: String): Unit = {
    import java.nio.file._

    val dir = Paths.get(path)
    if (Files.exists(dir)) {
      Files.walk(dir)
        .iterator()
        .asScala
        .toSeq
        .reverse
        .foreach(Files.delete)
    }
  }
}
