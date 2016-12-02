package hedgehog

import java.util.concurrent.{ConcurrentHashMap, Executors}

import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

import java.util.{Map => JavaMap}

class PerformanceSpec extends FunSpec with Matchers {
  describe("Performance") {
    it("on average, should be no more than three times as slow as ConcurrentHashMap putting large values") {
      val concurrentHashMap = new ConcurrentHashMap[Integer, String]
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]()
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(concurrentHashMap.put(i, "x" * 1024 * 1024)))
      val concurrentPutAverage = Timer.average
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(hedgehogMap.put(i, "x" * 1024 * 1024)))
      val hedgehogPutAverage = Timer.average

      hedgehogPutAverage shouldBe < (concurrentPutAverage * 3.0)
    }

    it("on average, should take no more than 2.5ms to retrieve an item for a given index") {
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]()
      Timer.reset()
      (0 until 500).foreach(i => hedgehogMap.put(i, "x" * 1024 * 1024))

      (0 until 500).foreach(i => Timer.time(hedgehogMap.get(i)) shouldEqual "x" * 1024 * 1024)

      Timer.average shouldBe < (2.5)
    }

    it("Supports storage of data larger than Max Integer") {
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]()
      (0 until 2047 + 100).foreach(i => hedgehogMap.put(i, "x" * 1024 * 1024))
      hedgehogMap.size shouldBe 2047 + 100
    }

    it("should be thread safe") {
      implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))
      val hedgehogMap = HedgehogMap.createEphemeralMap[String, String]()

      val inputTasks: Seq[Future[Unit]] = (0 until 3).map(_ => Future((0 until 100).foreach(_ => putValue(hedgehogMap))))
      while(!inputTasks.forall(_.isCompleted)) Thread.`yield`()
      throwableFromFutures(inputTasks).foreach(t => throw t)

      hedgehogMap.size shouldBe 300

      def verifyAValue(): Unit = {
        val randomKeyAndValue = hedgehogMap.keySet.toList.get(Random.nextInt(hedgehogMap.size))
        hedgehogMap.get(randomKeyAndValue) shouldEqual randomKeyAndValue
      }

      val outputTasks: Seq[Future[Unit]] = (0 until 3).map(_ => Future((0 until 100).foreach(_ => verifyAValue())))
      while(!outputTasks.forall(_.isCompleted)) Thread.`yield`()
      throwableFromFutures(outputTasks).foreach(t => throw t)
    }

    it("should perform concurrent puts comparable to ConcurrentHashMap") {
      val concurrentHashMap = new ConcurrentHashMap[String, String]
      val hedgehogMap = HedgehogMap.createEphemeralMap[String, String]()
      val executor = Executors.newFixedThreadPool(3)
      implicit val executionContext = ExecutionContext.fromExecutor(executor)

      Timer.reset()
      Timer.time {
        val tasks = (0 to 1000).map(_ => Future(putValue(concurrentHashMap)))
        while(!tasks.forall(_.isCompleted)) Thread.`yield`()
      }

      val concurrentHashMapTime = Timer.average

      Timer.reset()
      Timer.time {
        val tasks = (0 to 1000).map(_ => Future(putValue(hedgehogMap)))
        while(!tasks.forall(_.isCompleted)) Thread.`yield`()
      }

      val hedgehogMapTime = Timer.average

      executor.shutdown()
      hedgehogMapTime shouldBe < (concurrentHashMapTime * 3.0)
    }

    it("should perform concurrent puts comparable to a single threaded process") {
      val hedgehogMap1 = HedgehogMap.createEphemeralMap[String, String](concurrencyFactor = 1)
      val hedgehogMap2 = HedgehogMap.createEphemeralMap[String, String](concurrencyFactor = 3)
      val executor = Executors.newFixedThreadPool(3)
      implicit val executionContext = ExecutionContext.fromExecutor(executor)

      Timer.reset()
      Timer.time((0 to 1000).foreach(_ => putValue(hedgehogMap1)))

      val singleThreadTime = Timer.average

      Timer.reset()
      Timer.time {
        val tasks = (0 to 1000).map(_ => Future(putValue(hedgehogMap2)))
        while(!tasks.forall(_.isCompleted)) Thread.`yield`()
      }

      val multiThreadTime = Timer.average
      multiThreadTime shouldBe < (singleThreadTime / 2)
    }
  }

  private def putValue(map: JavaMap[String, String]): Unit = {
    val randomKeyAndValue = Random.nextString(5)
    map.put(randomKeyAndValue, randomKeyAndValue)
  }

  private def throwableFromFutures(futures: Seq[Future[_]]): Option[Throwable] =
    futures
      .flatMap(_.value)
      .filter(_.isFailure)
      .map(_.failed.get)
      .reduceOption[Throwable] { case (f1, f2) => f1.addSuppressed(f2); f1 }
}
