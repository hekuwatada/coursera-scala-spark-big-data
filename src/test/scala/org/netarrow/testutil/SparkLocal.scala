package org.netarrow.testutil

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkLocal {
  def appName: String

  private def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      //NOTE: Local in-process mode
      // driver is used for execution
      // uses as many threads as the number of processors available to JVM
      //@see https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-local.html
      .master("local[*]")
      .getOrCreate()
  }

  def withSparkSession(testBlock: (SparkSession) => Unit): Unit = {
    val ss = createSparkSession(appName)
    try {
      testBlock(ss)
    } finally {
      ss.sparkContext.stop()
    }
  }

  def withSparkContext(testBlock: (SparkContext) => Unit): Unit =
    withSparkSession((ss: SparkSession) => testBlock(ss.sparkContext))

}
