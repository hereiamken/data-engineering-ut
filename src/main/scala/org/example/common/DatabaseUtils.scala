package org.example.common

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatabaseUtils {
  /**
   * get connect props database
   *
   * @param user_name : user_name
   * @param pass_word : pass_word
   * @param driver    : driver
   * @param url       : url
   * @return
   */
  def getConnectProps(user_name: String, pass_word: String, driver: String, url: String): Map[String, String] = {
    Map[String, String](
      "user" -> user_name,
      "password" -> pass_word,
      "driver" -> driver,
      "url" -> url
    )
  }

  def getDBTypeByDriverClass(driverClass: String): String = driverClass match {
    case "oracle.jdbc.driver.OracleDriver" | "oracle.jdbc.OracleDriver" => "ORACLE"
    case "org.postgresql.Driver" => "POSTGRES"
    case "com.mysql.jdbc.Driver" | "com.mysql.cj.jdbc.Driver" => "MYSQL"
    case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => "SQLSERVER"
    case "org.mariadb.jdbc.Driver" => "MARIADB"
    case "org.apache.hive.jdbc.HiveDriver" => "HIVE"
    case "org.apache.hive.jdbc.HiveDriver2" => "HIVE2"
    case "org.sqlite.JDBC" => "SQLITE"
    case "com.snowflake.client.jdbc.SnowflakeDriver" => "SNOWFLAKE"
    case "org.h2.Driver" => "H2"
    case _ => "UNKNOWN"
  }

  def getDriverClassByDBType(dbType: String): String = dbType.toUpperCase match {
    case "ORACLE" => "oracle.jdbc.OracleDriver"
    case "POSTGRES" => "org.postgresql.Driver"
    case "MYSQL" => "com.mysql.cj.jdbc.Driver"
    case "SQLSERVER" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    case "MARIADB" => "org.mariadb.jdbc.Driver"
    case "HIVE" => "org.apache.hive.jdbc.HiveDriver"
    case "HIVE2" => "org.apache.hive.jdbc.HiveDriver2"
    case "SQLITE" => "org.sqlite.JDBC"
    case "SNOWFLAKE" => "com.snowflake.client.jdbc.SnowflakeDriver"
    case "H2" => "org.h2.Driver"
    case null => throw new NullPointerException("DB type cannot be null") // Added null check
    case _ => throw new IllegalArgumentException(s"Unsupported DB type: $dbType")
  }

  def readDB(spark: SparkSession, query: String, jdbcOptions: Map[String, String]): DataFrame = {
    val df = spark.read.format("jdbc").options(jdbcOptions).option("query", query).load()
    df
  }

  def writeDB(df: DataFrame, writeMode: String, tableName: String, jdbc: Map[String, String]): Unit = {
    df.write.format("jdbc")
      .mode(writeMode)
      .options(jdbc)
      .option("batchsize", "10000")
      .option("truncate", "true")
      .option("dbtable", tableName)
      .save()
  }

  def readParquet(spark: SparkSession, path: String, schema: StructType = null): DataFrame = {
    if (schema != null) {
      spark.read.schema(schema).parquet(path)
    } else {
      spark.read.parquet(path)
    }
  }

  def writeParquet(path: String, df: DataFrame, mode: String = "overwrite", numPartitions: Option[Int], partitionCols: Seq[String] = Seq.empty): Unit = {
    val repartitionDF = numPartitions match {
      case Some(n) if n > 0 => df.repartition(n)
      case _ => df
    }

    val writer = if (partitionCols.nonEmpty) {
      repartitionDF.write.mode(mode).partitionBy(partitionCols: _*)
    } else {
      repartitionDF.write.mode(mode)
    }
    writer.parquet(path)
  }
}
