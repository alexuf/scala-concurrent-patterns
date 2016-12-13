package alexuf.scala.concurrent

import java.util.concurrent.atomic.AtomicInteger

import alexuf.scala.concurrent.AsyncStream._
import org.scalatest.FlatSpec

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success


/**
  * Created by alexuf on 02/09/2016.
  */
class AsyncStreamSpec extends FlatSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def asyncStream(aStreamFrom: Int => AsyncStream[Int], count: Int = 1000000) = {

    type Validator[T] = T => Boolean

    @tailrec
    def consumeStream[T](s: Stream[T])
                        (validate: T => Unit): Unit = s match {
      case Stream.Empty =>
      case hs #:: ts =>
        //do something
        validate(hs)
        consumeStream(ts)(validate)
    }

    def consumeAStream[T](s: AsyncStream[T])
                               (validate: T => Unit): Future[Unit] = s.state.flatMap {
      case AsyncStream.EmptyState => Future.successful(())
      case h *:: tas =>
        //do something
        validate(h)
        consumeAStream(tas)(validate): Future[Unit]
    }

    val parts = 100

    it should "consume values" in {

      def validator: Int => Unit = {
        val state = new AtomicInteger(0)
        x: Int => assert(x == state.getAndIncrement())
      }

      {
        val validate = validator
        consumeStream(Stream.from(0).take(count))(validate)
        validate(count)
      }

      {
        val validate = validator
        Await.result(
          consumeAStream(aStreamFrom(0).take(count))(validate), Duration.Inf
        )
        validate(count)
      }
    }

    it should "foreach values" in {

      def validator: Int => Unit = {
        val state = new AtomicInteger(0)
        x: Int => assert(x == state.getAndIncrement())
      }
      
      {
        val validate = validator
        for {
          value <- Stream.from(0).take(count)
        } {
          validate(value)
        }
        validate(count)
      }

      {
        val validate = validator
        Await.result(
          for {
            value <- aStreamFrom(0).take(count)
          } {
            validate(value)
          }, Duration.Inf
        )
        validate(count)
      }
    }

    it should "map values" in {

      def validator: String => Unit = {
        val state = new AtomicInteger(0)
        x: String => assert(x == s"${state.getAndIncrement()}")
      }

      {
        val validate = validator
        val stream = for {
          value <- Stream.from(0).take(count)
        } yield s"$value"
        consumeStream(stream)(validate)
        validate(s"$count")
      }

      {
        val validate = validator
        val aStream = for {
          value <- aStreamFrom(0).take(count)
        } yield s"$value"
        Await.result(
          consumeAStream(aStream)(validate), Duration.Inf
        )
        validate(s"$count")
      }
    }

    it should "map values withFilter" in {

      def validator: String => Unit = {
        val state = new AtomicInteger(0)
        x: String => assert(x == s"${state.getAndAdd(2)}")
      }

      {
        val validate = validator
        val stream = for {
          value <- Stream.from(0).take(count) if value % 2 == 0
        } yield s"$value"
        consumeStream(stream)(validate)
        validate(s"$count")
      }

      {
        val validate = validator
        val aStream = for {
          value <- aStreamFrom(0).take(count) if value % 2 == 0
        } yield s"$value"
        Await.result(
          consumeAStream(aStream)(validate), Duration.Inf
        )
        validate(s"$count")
      }
    }

    it should "flatMap values" in {

      def validator: ((Int, Int)) => Unit = {
        val state = new AtomicInteger(0)

        x: (Int, Int) => {
          if (count != 0) {
            val current = state.getAndIncrement()
            val part = current / (count / parts)
            val value = current % (count / parts)
            assert(x == (part, value))
          } else {
            assert(0 == state.get())
          }

        }
      }

      {
        val validate = validator
        val stream = for {
          part <- Stream.from(0).take(parts)
          value <- Stream.from(0).take(count / parts)
        } yield (part, value)
        consumeStream(stream)(validate)
        validate((parts, 0))
      }

      {
        val validate = validator
        val aStream = for {
          part <- aStreamFrom(0).take(parts)
          value <- aStreamFrom(0).take(count / parts)
        } yield (part, value)
        Await.result(
          consumeAStream(aStream)(validate), Duration.Inf
        )
        validate((parts, 0))
      }
    }

    it should "flatMap values withFilter" in {

      def validator: ((Int, Int)) => Unit = {
        val state = new AtomicInteger(0)

        x: (Int, Int) => {
          if (count != 0) {
            val current = state.getAndIncrement()
            val part = 2 * (current / (count / parts))
            val value = current % (count / parts)
            assert(x == (part, value))
          } else {
            assert(0 == state.get())
          }
        }
      }

      {
        val validate = validator
        val stream = for {
          part <- Stream.from(0).take(parts * 2) if part % 2 == 0
          value <- Stream.from(0).take(count / parts)
        } yield (part, value)
        consumeStream(stream)(validate)
        validate((parts * 2, 0))
      }

      {
        val validate = validator
        val aStream = for {
          part <- aStreamFrom(0).take(parts * 2) if part % 2 == 0
          value <- aStreamFrom(0).take(count / parts)
        } yield (part, value)
        Await.result(
          consumeAStream(aStream)(validate), Duration.Inf
        )
        validate((parts * 2, 0))
      }
    }

    it should "foldLeft values" in {
      val sum = Stream.from(0).take(count).sum

      assertResult(sum) {
        Await.result(
          aStreamFrom(0).take(count).foldLeft(0)(_ + _), Duration.Inf
        )
      }
    }

    it should "convert itself to Future[Seq]" in {
      val seq = Stream.from(0).take(math.min(count, 100)).force

      assertResult(seq) {
        Await.result(
          aStreamFrom(0).take(math.min(count, 100)).toSeq, Duration.Inf
        )
      }
    }

    it should "group " in {
      val validator = new AtomicInteger(0)
      for {
        group <- Stream.from(0).take(count).grouped(math.max(count / parts, 1))
      } {
        assert(group.size <= count / parts)
        validator.incrementAndGet()
      }
      assert(validator.get == count / math.max(count / parts, 1))

      validator.set(0)
      Await.result(
        for {
          group <- aStreamFrom(0).take(count).grouped(math.max(count / parts, 1))
        } {
          assert(Await.result(group.foldLeft(0)((s, a) => s + 1), Duration.Inf) <= count / parts)
          validator.incrementAndGet()
        }, Duration.Inf
      )
      assert(validator.get == count / math.max(count / parts, 1))
    }
  }

  "Empty" should behave like asyncStream(_ => AsyncStream.empty[Int], 0)

  def consAStreamFrom(start: Int): AsyncStream[Int] = {
    start *:: consAStreamFrom(start + 1)
  }

  "Cons" should behave like asyncStream(consAStreamFrom)

  def futureConsAStream(start: Int): AsyncStream[Int] = {
    val promiseStream = Promise[AsyncStream[Int]]
    val stream = AsyncStream.futureCons(promiseStream.future)
    promiseStream.complete(Success(start *:: futureConsAStream(start + 1)))
    stream
  }

  "FutureCons" should behave like asyncStream(futureConsAStream)

  "Converted" should behave like asyncStream((start: Int) => Stream.from(start).toAsyncStream)
}
