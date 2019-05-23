package org.netarrow.w4

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

import scala.collection.immutable

class DataFrameSpec extends FunSpec with Matchers with SparkLocal {
  override def appName: String = "DataFrameSpec"

  //NOTE: JSON must be line delimited format
  val jsonFile: URL = getClass.getClassLoader.getResource("persons.json")

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
        val df: DataFrame = ss.read.json(jsonFile.getFile)
        val rows: Array[Row] = df.collect()

        println(df.show())

        rows.size shouldBe 3
      }
    }

    it("reads Person data set from a file") {
      withSparkSession { ss =>
        import ss.implicits._
        val schema = Encoders.product[Person].schema

        val ds: Dataset[Person] = ss.read
          .schema(schema)
          .json(jsonFile.getFile)
          .as[Person]

        ds.first shouldBe Person(1, "foo", "London", "United Kingdom")
      }
    }

    it("reads slimmed down objects from a file") {
      withSparkSession { ss =>
        import ss.implicits._

        val ds: Dataset[SlimPerson] = ss.read
          .json(jsonFile.getFile)
          .as[SlimPerson]

        val ret: immutable.Seq[SlimPerson] = ds.collect().toList
        ret shouldBe List(SlimPerson("foo"), SlimPerson("bar"), SlimPerson("hoge"))
      }
    }
  }

  describe("DataFrame with SQL") {
    it("accepts SQL queries") {
      withSparkSession { ss =>
        val df: DataFrame = ss.read.json(jsonFile.getFile)

        // Register temporary view
        df.createOrReplaceTempView("people")
        val df2: DataFrame = ss.sql("SELECT * FROM people WHERE id > 1")

        val rows: Array[Row] = df2.collect()

        rows.size shouldBe 2
      }
    }

    it("finds and sorts employees by id who live in a particular city") {
      withSparkSession { ss =>
        import ss.implicits._

        val employees = Seq(
          Employee(1, "fname1", "lname1", 21, "London"),
          Employee(2, "fname2", "lname2", 30, "Paris"),
          Employee(3, "fname3", "lname3", 27, "London"),
          Employee(4, "fname4", "lname4", 40, "New York"),
          Employee(5, "fname5", "lname5", 32, "New York"),
          Employee(5, "fname5", "lname5", 19, "London")
        )

        val df: DataFrame = ss.sparkContext.parallelize(employees).toDF()
        df.show()

        df.createOrReplaceTempView("employees")
        def findEmployees(city: String): DataFrame =
          ss.sql(s"""SELECT id, lname FROM employees WHERE city == "$city" ORDER BY id""")

        val employees1 = findEmployees("New York")
        employees1.show()
        employees1.collect().size shouldBe 2

        val employees2 = findEmployees("London")
        employees2.show()
        employees2.collect().size shouldBe 3
      }
    }
  }
}
