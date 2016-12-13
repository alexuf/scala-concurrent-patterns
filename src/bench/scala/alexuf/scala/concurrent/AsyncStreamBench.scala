package alexuf.scala.concurrent

import java.util.concurrent.atomic.AtomicInteger

import alexuf.scala.concurrent.AsyncStream._
import org.scalameter.api._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

/**
  * Created by alexuf on 26/09/2016.
  */
class AsyncStreamBench extends Bench.LocalTime {

  import scala.concurrent.ExecutionContext.Implicits.global

  private lazy val sizes = Gen.exponential("size")(10, 100000, 100)
  import org.scalameter.picklers.noPickler._
  private lazy val aStreams = Gen.enumeration("aStream")({
    Stream.from(0).toAsyncStream
  },{
    def futureConsAStream(start: Int): AsyncStream[Int] = {
      val promiseStream = Promise[AsyncStream[Int]]
      val stream = AsyncStream.futureCons(promiseStream.future)
      promiseStream.complete(Success(start *:: futureConsAStream(start + 1)))
      stream
    }
    futureConsAStream(0)
  })

  @tailrec
  private def consumeStream[T](s: Stream[T])
                      (validate: T => Boolean): Unit = s match {
    case Stream.Empty =>
    case hs #:: ts =>
      //do something
      assert(validate(hs))
      consumeStream(ts)(validate)
  }

  private def consumeAStream[T](s: AsyncStream[T])
                       (validate: T => Boolean): Future[Unit] = s.state.flatMap {
    case AsyncStream.EmptyState => Future.successful(())
    case h *:: tas =>
      //do something
      assert(validate(h))
      consumeAStream(tas)(validate): Future[Unit]
  }

  performance of "consume" in {

    def validator = {
      val state = new AtomicInteger(0)
      x: Int => state.compareAndSet(x, x + 1)
    }

    performance of "Stream" in {
      using(sizes) in { size =>
        consumeStream(Stream.from(0).take(size))(validator)
      }
    }

    performance of "AsyncStreams" in {
      using(Gen.crossProduct(aStreams, sizes)) in { case (stream, size) =>
        Await.result(
          consumeAStream(stream.take(size))(validator), Duration.Inf
        )
      }
    }
  }

  performance of "foreach" in {

    def validator = {
      val state = new AtomicInteger(0)
      x: Int => state.compareAndSet(x, x + 1)
    }

    performance of "Stream" in {
      using(sizes) in { size =>
        val validate = validator
        for {
          value <- Stream.from(0).take(size)
        } {
          assert(validate(value))
        }
      }
    }

    performance of "AsyncStreams" in {
      using(Gen.crossProduct(aStreams, sizes)) in { case (stream, size) =>
        val validate = validator
        Await.result(
          for {
            value <- stream.take(size)
          } {
            assert(validate(value))
          }, Duration.Inf
        )
      }
    }
  }

  performance of "map" in {
    def validator = {
      val state = new AtomicInteger(0)
      x: String => x == s"${state.getAndIncrement()}"
    }

    performance of "Stream" in {
      using(sizes) in { size =>
        val stream = for {
          value <- Stream.from(0).take(size)
        } yield s"$value"
        consumeStream(stream)(validator)
      }
    }

    performance of "AsyncStreams" in {
      using(Gen.crossProduct(aStreams, sizes)) in { case (stream, size) =>
        val aStream = for {
          value <- stream.take(size)
        } yield s"$value"

        Await.result(
          consumeAStream(aStream)(validator), Duration.Inf
        )
      }
    }
  }

  performance of "map withFilter" in {
    def validator = {
      val state = new AtomicInteger(0)
      x: String => x == s"${state.getAndAdd(2)}"
    }

    performance of "Stream" in {
      using(sizes) in { size =>
        val stream = for {
          value <- Stream.from(0).take(size) if value % 2 == 0
        } yield s"$value"
        consumeStream(stream)(validator)
      }
    }

    performance of "AsyncStreams" in {
      using(Gen.crossProduct(aStreams, sizes)) in { case (stream, size) =>
        val aStream = for {
          value <- stream.take(size) if value % 2 == 0
        } yield s"$value"

        Await.result(
          consumeAStream(aStream)(validator), Duration.Inf
        )
      }
    }
  }

  performance of "flatMap" in {

    def validator(parts: Int, size: Int) = {
      val state = new AtomicInteger(0)

      x: (Int, Int) => {
        val current = state.getAndIncrement()
        val part = current / (size / parts)
        val value = current % (size / parts)
        x == (part, value)
      }
    }

    val splits = for {
      size <- sizes
      parts <- Gen.single("parts")(math.log10(size).toInt)
    } yield (parts, size)

    performance of "Stream" in {
      using(splits) in { case (parts, size) =>
        val stream = for {
          part <- Stream.from(0).take(parts)
          value <- Stream.from(0).take(size / parts)
        } yield (part, value)

        consumeStream(stream)(validator(parts, size))
      }
    }

    performance of "AsyncStreams" in {
      val streams = Gen.crossProduct(aStreams, aStreams)
      using(Gen.crossProduct(streams, splits)) in { case ((stream1, stream2), (parts, size)) =>
        val aStream = for {
          part <- stream1.take(parts)
          value <- stream2.take(size / parts)
        } yield (part, value)

        Await.result(
          consumeAStream(aStream)(validator(parts, size)), Duration.Inf
        )
      }
    }
  }

  performance of "flatMap withFilter" in {

    def validator(parts: Int, size: Int) = {
      val state = new AtomicInteger(0)

      x: (Int, Int) => {
        val current = state.getAndIncrement()
        val part = 2 * (current / (size / parts))
        val value = current % (size / parts)
        x == (part, value)
      }
    }

    val splits = for {
      size <- sizes
      parts <- Gen.single("parts")(math.log10(size).toInt)
    } yield (parts, size)

    performance of "Stream" in {
      using(splits) in { case (parts, size) =>
        val stream = for {
          part <- Stream.from(0).take(parts) if part % 2 == 0
          value <- Stream.from(0).take(size / parts)
        } yield (part, value)

        consumeStream(stream)(validator(parts, size))
      }
    }

    performance of "AsyncStreams" in {
      val streams = Gen.crossProduct(aStreams.rename("aStream" -> "aStream1"), aStreams.rename("aStream" -> "aStream2"))
      using(Gen.crossProduct(streams, splits)) in { case ((stream1, stream2), (parts, size)) =>
        val aStream = for {
          part <- stream1.take(parts) if part % 2 == 0
          value <- stream2.take(size / parts)
        } yield (part, value)

        Await.result(
          consumeAStream(aStream)(validator(parts, size)), Duration.Inf
        )
      }
    }
  }
  
  performance of "foldLeft" in {

    performance of "Stream" in {
      using(sizes) in { size =>
        Stream.from(0).take(size).sum
      }
    }

    performance of "AsyncStreams" in {
      
      using(Gen.crossProduct(aStreams, sizes)) in { case (stream, size) =>
        Await.result(
          stream.take(size).foldLeft(0)(_ + _), Duration.Inf
        )
      }
    }
  }
}
