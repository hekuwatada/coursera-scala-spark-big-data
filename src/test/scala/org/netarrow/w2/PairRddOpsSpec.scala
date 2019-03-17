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
    it("runs reduceByKey") {
      withSparkContext { sc =>
        val events = Seq(
          Event("org1", "event1", 100),
          Event("org2", "event2", 250),
          Event("org1", "event3", 700)
        )
        val rdd: RDD[(String, Int)] = createPairRdd(events)((event: Event) => (event.organizer, event.budget))(sc)
        val reducedRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        val costs: Array[(String, Int)] = reducedRdd.collect()
        costs should contain theSameElementsAs Array("org1" -> 800, "org2" -> 250)
      }
    }
  }

}
