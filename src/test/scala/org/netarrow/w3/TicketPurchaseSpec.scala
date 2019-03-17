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
    
    it("calculates how many trips were made and how much money was spent by each visitor") {
      withSparkContext { sc =>
        val purchasesRdd: RDD[TicketPurchase] = sc.parallelize(purchases)

        val pricePairRdd: RDD[(Int, Double)] = purchasesRdd.map(p => (p.customerId, p.price))
        val groupedPrice: RDD[(Int, Iterable[Double])] = pricePairRdd.groupByKey()
        val tripsCost: RDD[(Int, (Int, Double))] = groupedPrice.mapValues(p => (p.size, p.sum))
        tripsCost.collect() should contain theSameElementsAs Map(1 -> (3, 200), 2 -> (2, 220), 3 -> (2, 295))
      }
    }

  }
}
