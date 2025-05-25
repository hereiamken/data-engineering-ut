package org.example.base

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.config.SparkContextCommon
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterEach, Outcome, TestSuiteMixin}
import org.slf4j.{Logger, LoggerFactory}

trait SparkTestBase extends AnyFlatSpec with BeforeAndAfterEach with Matchers with TestSuiteMixin {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()

  def sc: SparkContext = SparkContextCommon.sparkContext

  // Override withFixture to get test data (name, etc.)
  override def withFixture(test: NoArgTest): Outcome = {
    val testClassName = this.getClass.getSimpleName
    val testMethodName = test.name

    logger.info(s"--------------------------------------------------------------------")
    logger.info(s"STARTING TEST - Class: $testClassName | Test: $testMethodName")

    val outcome = super.withFixture(test) // run the actual test

    logger.info(s"ENDING TEST - Class: $testClassName | Test: $testMethodName")
    logger.info(s"--------------------------------------------------------------------")

    outcome
  }

  def assertDataFrameEquality(actual: DataFrame, expected: DataFrame): Assertion = {
    val actualCollection = actual.collect().toSeq
    val expectedCollection = expected.collect().toSeq
    actualCollection should contain theSameElementsAs expectedCollection
  }
}
