package org.example

import org.example.config.SparkContextCommon
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AppTests extends AnyFunSuite with BeforeAndAfterAll {

  /**
   * This method runs ONCE before any test cases in the suite are executed.
   * It initializes a shared SparkContext for the entire test suite.
   * Pre-initializing the SparkContext reduces overhead and avoids redundant setups in each test.
   */
  override def beforeAll(): Unit = {
    println("Starting SparkContext for the entire test suite...")
    val _ = SparkContextCommon.sparkContext
    println("SparkContext started.")
  }

  /**
   * This method runs ONCE after all test cases in the suite have finished.
   * It stops the shared SparkContext to release resources.
   * Ensures proper cleanup and prevents resource leaks after tests complete.
   */
  override def afterAll(): Unit = {
    println("Stopping SparkContext after the entire test suite...")
    SparkContextCommon
      .stop() // Dừng SparkContext
    println("SparkContext stopped.")
  }
}
