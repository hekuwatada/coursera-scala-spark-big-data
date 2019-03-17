package org.netarrow.w3

import org.apache.spark.rdd.RDD
import org.netarrow.testutil.SparkLocal
import org.scalatest.{FunSpec, Matchers}

class TicketPurchaseSpec extends FunSpec with Matchers with SparkLocal {
  override def appName: String = "TicketPurchaseSpec"

  describe("data shuffle") {
    val purchases = Seq(
      TicketPurchase(1, "destA", 100),
      TicketPurchase(2, "destA", 100),
      TicketPurchase(1, "destB", 50),
      TicketPurchase(1, "destB", 50),
      TicketPurchase(2, "destD", 120),
      TicketPurchase(3, "destC", 175),
      TicketPurchase(3, "destD", 120)
    )

    it("calculates how many trips were made by each visitor") {
      withSparkContext { sc =>
        val purchasesRdd: RDD[TicketPurchase] = sc.parallelize(purchases)

        val destPairRdd: RDD[(Int, String)] = purchasesRdd.map(p => (p.customerId, p.destination))
        val groupedDest: RDD[(Int, Iterable[String])] = destPairRdd.groupByKey()
        val countDest: RDD[(Int, Int)] = groupedDest.mapValues(d => d.toSeq.size)
        countDest.collect() should contain theSameElementsAs Map(1 -> 3, 2 -> 2, 3 -> 2)
      }
    }

    it("calculates how much money was spent by each visitor") {
      withSparkContext { sc =>
        val purchasesRdd: RDD[TicketPurchase] = sc.parallelize(purchases)

        val pricePairRdd: RDD[(Int, Double)] = purchasesRdd.map(p => (p.customerId, p.price))
        val groupedPrice: RDD[(Int, Iterable[Double])] = pricePairRdd.groupByKey()
        val totalPrice: RDD[(Int, Double)] = groupedPrice.mapValues(p => p.sum)
        totalPrice.collect() should contain theSameElementsAs Map(1 -> 200, 2 -> 220, 3 -> 295)
      }
    }

  }
}
