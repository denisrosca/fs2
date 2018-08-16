package fs2.async

import cats.effect.IO
import fs2._
import fs2.Stream._

import scala.concurrent.duration._

class TopicSpec extends AsyncFs2Spec {

  "Topic" - {

    "subscribers see all elements published" in {

      val count = 100
      val subs = 10

      val expected = (for { i <- 0 until subs } yield i).map { idx =>
        idx -> (for { i <- -1 until count } yield i).toVector
      }.toMap

      val value =
        for {
          topic <- Stream.eval(async.topic[IO, Int](-1))
          publisher = Stream.sleep[IO](1.second) ++ Stream
            .range(0, count)
            .covary[IO]
            .through(topic.publish)
          subscriber = topic.subscribe(Int.MaxValue).take(count + 1).fold(Vector.empty[Int]) {
            _ :+ _
          }
          result <- (Stream
            .range(0, subs)
            .map(idx => subscriber.map(idx -> _)) ++ publisher.drain)
            .parJoin(subs + 1)
        } yield result

      value
        .compile
        .toVector
        .unsafeToFuture()
        .map{result =>
          result.toMap.size shouldBe subs
          result.toMap shouldBe expected
        }
    }

    "synchronous publish" in {
      pending // TODO I think there's a race condition on the signal in this test
      val count = 100
      val subs = 10

      val test = for {
        topic <- Stream.eval(async.topic[IO, Int](-1))
        signal <- Stream.eval(async.signalOf[IO, Int](0))

        publisher = Stream.sleep[IO](1.second) ++ Stream
          .range(0, count)
          .covary[IO]
          .flatMap(i => eval(signal.set(i)).map(_ => i))
          .through(topic.publish)

        subscriber = topic
          .subscribe(1)
          .take(count + 1)
          .flatMap { is =>
            eval(signal.get).map(is -> _)
          }
          .fold(Vector.empty[(Int, Int)]) { _ :+ _ }

        result <- (Stream
          .range(0, subs)
          .map(idx => subscriber.map(idx -> _)) ++ publisher.drain)
          .parJoin(subs + 1)

      } yield result

      test
        .compile
        .toVector
        .unsafeToFuture()
        .map{result =>
          result.foreach {
            case (_, subResults) =>
              val diff: Set[Int] = subResults.map {
                case (read, state) => Math.abs(state - read)
              }.toSet
              assert(diff.min == 0 || diff.min == 1)
              assert(diff.max == 0 || diff.max == 1)
          }
          result.toMap.size shouldBe subs
        }
    }
  }
}
