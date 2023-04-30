package org.apache.flink

import java.io.{File, PrintWriter}
import java.time.Instant
import scala.util.Random
import scala.collection.mutable.ListBuffer


case class SampleTransaction(
                              TransactionId: Long,
                              isRequest: Boolean,
                              eventTime: Long
                            ) {
}





object Generator {


  val writer = new PrintWriter(new File("/home/ejada/Downloads/Transactions"))
  val transactionsNumber = 10
  var transactions = new ListBuffer[SampleTransaction]()


  def main(args: Array[String]): Unit = {
    val random = new Random()
    //Initial transactions
    var lastDelay = random.nextInt(20)
    transactions.append(SampleTransaction(TransactionId = 0, isRequest = true, eventTime = lastDelay))

    if (random.nextInt(10) + 1 != 1) {
      transactions.append(SampleTransaction(TransactionId = 0, isRequest = false, eventTime = transactions(0).eventTime + (random.nextInt(60) + 1) * 1000))
    }

    for (i <- 1 until transactionsNumber) {

      if (random.nextInt(6) == 0) {
        transactions.append(SampleTransaction(TransactionId = i, isRequest = true, eventTime = lastDelay))
      }
      else {
        lastDelay = lastDelay + (random.nextInt(60) + 1) * 10
        transactions.append(SampleTransaction(TransactionId = i, isRequest = true, eventTime = lastDelay))
      }


    }
    transactions.sortBy(_.eventTime).foreach(request => writer.write(request.TransactionId.toString + "," + request.isRequest.toString + "," + request.eventTime + "\n"))
    writer.close()
  }
}


