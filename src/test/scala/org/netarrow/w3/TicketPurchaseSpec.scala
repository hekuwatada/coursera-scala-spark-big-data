package org.netarrow.w3

import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, RangePartitioner, ShuffleDependency}
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

class TicketPurchaseSpec extends FunSpec with Matchers with SparkLocal {
  override def appName: String = "TicketPurchaseSpec"

  describe("Data shuffle") {
    val purchases = Seq(
      TicketPurchase(1, "destA", 100),
      TicketPurchase(2, "destA", 100),
      TicketPurchase(1, "destB", 50),
      TicketPurchase(1, "destB", 50),
      TicketPurchase(2, "destD", 120),
      TicketPurchase(3, "destC", 175),
      TicketPurchase(3, "destD", 120)
    )

    //TODO: fix compiler warnings

    it("calculates how many trips were made and how much money was spent by each visitor - unnecessarily groupBy + reduce") {
      withSparkContext { sc =>
        val purchasesRdd: RDD[TicketPurchase] = sc.parallelize(purchases)

        val pricePairRdd: RDD[(Int, Double)] = purchasesRdd.map(p => (p.customerId, p.price))
        val groupedPrice: RDD[(Int, Iterable[Double])] = pricePairRdd.groupByKey()
        val tripsCost: RDD[(Int, (Int, Double))] = groupedPrice.mapValues(p => (p.size, p.sum))

        groupedPrice.dependencies.head shouldBe a[ShuffleDependency[_, _, _]]
        //TODO: why not ShuffleDependency and OneToOneDependency?
        tripsCost.dependencies.size shouldBe 1
        tripsCost.dependencies.head shouldBe a[OneToOneDependency[_]]

        /*
        (8) MapPartitionsRDD[3] at mapValues at TicketPurchaseSpec.scala:28 []
         |  ShuffledRDD[2] at groupByKey at TicketPurchaseSpec.scala:27 []
         +-(8) MapPartitionsRDD[1] at map at TicketPurchaseSpec.scala:26 []
            |  ParallelCollectionRDD[0] at parallelize at TicketPurchaseSpec.scala:24 []
         */
        println(tripsCost.toDebugString)

        tripsCost.collect() should contain theSameElementsAs Map(1 -> (3, 200), 2 -> (2, 220), 3 -> (2, 295))
      }
    }

    it("calculates how many trips were made and how much money was spent by each visitor - reduced data shuffle by reducing data set first with reduceByKey()") {
      withSparkContext { sc =>
        val purchasesRdd: RDD[TicketPurchase] = sc.parallelize(purchases)

        val tripPricePairRdd: RDD[(Int, (Int, Double))] = purchasesRdd.map(p => (p.customerId, (1, p.price)))

        // reduceByKey() - reduces values on the same node first, then across nodes per key
        // therefore optimized and no need to groupByKey()
        val tripsCost: RDD[(Int, (Int, Double))] = tripPricePairRdd
            .reduceByKey { case ((xTrips, xCost), (yTrips, yCost)) => (xTrips + yTrips, xCost + yCost) }

        // RDD.dependencies to check wide (causing shuffle) or narrow dependencies
        // ShuffleDependency = wide dependency
        tripsCost.dependencies.head shouldBe a[ShuffleDependency[_, _, _]] // this is not good for prod
        /*
        (8) ShuffledRDD[2] at reduceByKey at TicketPurchaseSpec.scala:30 []
         +-(8) MapPartitionsRDD[1] at map at TicketPurchaseSpec.scala:25 []
            |  ParallelCollectionRDD[0] at parallelize at TicketPurchaseSpec.scala:23 []
         */
        println(tripsCost.toDebugString)

        tripsCost.collect() should contain theSameElementsAs Map(1 -> (3, 200), 2 -> (2, 220), 3 -> (2, 295))
      }
    }
  }

  describe("Partition for pair RDD") {
    val purchases = Seq(
      TicketPurchase(4, "destA", 100),
      TicketPurchase(100, "destA", 100),
      TicketPurchase(3, "destB", 50),
      TicketPurchase(80, "destB", 50),
      TicketPurchase(12, "destD", 120),
      TicketPurchase(1, "destC", 175),
      TicketPurchase(345, "destD", 120),
      TicketPurchase(100, "destD", 120),
      TicketPurchase(3, "destD", 120),
      TicketPurchase(4, "destD", 120)
    )

    it("adds range partition") {
      withSparkContext { sc =>
        val pairRdd: RDD[(Int, (Int, Double))] = sc.parallelize(purchases).map(p => (p.customerId, (1, p.price)))

        //NOTE: To use range partitioning:
        // keys must have ordering
        // pair RDD is passed in for rangePartition() to sample data and figure out the best range of partitioning
        val rangePartition: RangePartitioner[Int, (Int, Double)] = new RangePartitioner(8, pairRdd)

        val cachedRdd: RDD[(Int, (Int, Double))] = pairRdd
          .partitionBy(rangePartition)
          .persist() //NOTE: once partitioned, keep data in memory

        val tripsCost: RDD[(Int, (Int, Double))] = cachedRdd
          .reduceByKey { case ((xTrips, xCost), (yTrips, yCost)) => (xTrips + yTrips, xCost + yCost) }

        // RDD.dependencies to check wide (causing shuffle) or narrow dependencies
        // OneToOneDependency = narrow dependency
        tripsCost.dependencies.head shouldBe a[OneToOneDependency[_]]
        /*
        (7) MapPartitionsRDD[5] at reduceByKey at TicketPurchaseSpec.scala:72 []
         |  ShuffledRDD[4] at partitionBy at TicketPurchaseSpec.scala:70 []
         +-(8) MapPartitionsRDD[1] at map at TicketPurchaseSpec.scala:63 []
            |  ParallelCollectionRDD[0] at parallelize at TicketPurchaseSpec.scala:63 []
         */
        println(tripsCost.toDebugString)

        tripsCost.collect() should contain theSameElementsAs
          Map(
            1 -> (1, 175.0),
            3 -> (2, 170.0),
            4 -> (2, 220.0),
            100 -> (2, 220.0),
            80 -> (1, 50.0),
            12 -> (1, 120.0),
            345 -> (1, 120.0)
          )
      }
    }
  }
}
