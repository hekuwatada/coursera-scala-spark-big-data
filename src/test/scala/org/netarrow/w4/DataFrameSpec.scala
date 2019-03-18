package org.netarrow.w4

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

class DataFrameSpec extends FunSpec with Matchers with SparkLocal {
  override def appName: String = "DataFrameSpec"

  describe("DataFrame") {
    it("creates DataFrame from RDD with column names") {
      withSparkSession { ss =>
        import ss.implicits._

        val data: Seq[(Int, String, String, String)] = Seq(
          (1, "Monmouth Cafe", "London", "United Kingdom"),
          (2, "Taylor St Baristas", "London", "United Kingdom"),
          (3, "Mame Coffee", "Toyama", "Japan")
        )
        val rdd: RDD[(Int, String, String, String)] = ss.sparkContext.parallelize(data)
        //NOTE: DataFrame has NO type
        val df: DataFrame = rdd.toDF("id", "name", "city", "country")

        val rows: Array[Row] = df.collect()
        rows.size shouldBe 3
      }
    }

    it("creates DataFrame from RDD of case class") {
      withSparkSession { ss =>
        import ss.implicits._

        val persons = Seq(
          Person(1, "foo", "London", "United Kingdom"),
          Person(2, "bar", "London", "United Kingdom")
        )

        val rdd: RDD[Person] = ss.sparkContext.parallelize(persons)
        val df: DataFrame = rdd.toDF()
        val rows: Array[Row] = df.collect()

        rows.size shouldBe 2
      }
    }

    it("creates DataFrame from a file") {
      withSparkSession { ss =>
        //NOTE: JSON must be line delimited format
        val jsonFile: URL = getClass.getClassLoader.getResource("persons.json")
        val df: DataFrame = ss.read.json(jsonFile.getFile)
        val rows: Array[Row] = df.collect()

        rows.size shouldBe 3
      }
    }
  }

  describe("DataFrame with SQL") {
    it("accepts SQL queries") {
      withSparkSession { ss =>
        val jsonFile: URL = getClass.getClassLoader.getResource("persons.json")
        val df: DataFrame = ss.read.json(jsonFile.getFile)

        // Register temporary view
        df.createOrReplaceTempView("people")
        val df2: DataFrame = ss.sql("SELECT * FROM people WHERE id > 1")

        val rows: Array[Row] = df2.collect()

        rows.size shouldBe 2
      }
    }
  }
}
