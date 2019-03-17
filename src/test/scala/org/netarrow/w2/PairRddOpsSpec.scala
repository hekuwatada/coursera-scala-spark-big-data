package org.netarrow.w2

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

import scala.reflect.ClassTag

class PairRddOpsSpec extends FunSpec with Matchers with SparkLocal {
  val appName: String = "PairRddOpsSpec"

  //NOTE: compiler cannot get type information from RDD[T], hence requiring implicit parameter ClassTag[T]
  // A : ClassTag is context bound and equivalent to (implicit ct: ClassTag[T])
  def createPairRdd[A : ClassTag, K, V](col: Seq[A])(f: A => (K, V))(sc: SparkContext): RDD[(K, V)] =
    sc.parallelize(col).map(f)

  describe("Pair RDD operations") {
    val events = Seq(
      Event("org1", "event1", 100),
      Event("org2", "event2", 250),
      Event("org1", "event3", 700)
    )

    it("runs reduceByKey") {
      withSparkContext { sc =>
        val rdd: RDD[(String, Int)] = createPairRdd(events)((event: Event) => (event.organizer, event.budget))(sc)
        val reducedRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        val costs: Array[(String, Int)] = reducedRdd.collect()
        costs should contain theSameElementsAs Array("org1" -> 800, "org2" -> 250)
      }
    }

    it("runs countByKey - action") {
      withSparkContext { sc =>
        val rdd: RDD[(String, Int)] = createPairRdd(events)((event: Event) => (event.organizer, event.budget))(sc)
        val costs: collection.Map[String, Long] = rdd.countByKey()
        costs should contain theSameElementsAs Map("org1" -> 2, "org2" -> 1)
      }
    }

    it("runs keys - transformation so number of keys could be large") {
      withSparkContext { sc =>
        val rdd: RDD[(String, Int)] = createPairRdd(events)((event: Event) => (event.organizer, event.budget))(sc)
        val keys: RDD[String] = rdd.keys
        //NOTE: keys could have duplicates
        keys.collect() should contain theSameElementsAs Array("org1", "org2", "org1")
      }
    }

    it("computes average budget per event organizer") {
      withSparkContext { sc =>
        val rdd: RDD[(String, Event)] = createPairRdd(events)(event => (event.organizer, event))(sc)
        // (total cost, total numb of events)
        val reducedRdd: RDD[(String, (Int, Int))] = rdd
            .mapValues(event => (event.budget, 1))
            .reduceByKey { case ((xBudget, xNum), (yBudget, yNum)) => (xBudget + yBudget, xNum + yNum) }

        val average: RDD[(String, Double)] = reducedRdd
            .mapValues { case (cost, eventCount) =>  cost / eventCount }

        average.collect() should contain theSameElementsAs Array("org1" -> 400, "org2" -> 250)
      }
    }
  }

}
