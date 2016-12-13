package alexuf.scala.concurrent

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexuf on 06/09/2016.
  */
sealed trait AsyncStream[+A] {

  def state(implicit ec: ExecutionContext): Future[AsyncStream.State[A]]

  def map[B](f: A => B)(implicit ec: ExecutionContext): AsyncStream[B]
  def flatMap[B](f: A => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B]
  def foreach(f: A => Unit)(implicit ec: ExecutionContext): Future[Unit] =
    foldLeft(())((_, a) => f(a))
  def withFilter(p: A => Boolean)(implicit ec: ExecutionContext): AsyncStream[A]

  def ++[B >: A](that: => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B]

  def foldLeft[B](z: B)(f: (B, A) => B)(implicit ec: ExecutionContext): Future[B]
  def take(n: Int)(implicit ec: ExecutionContext): AsyncStream[A]
  def drop(n: Int)(implicit ec: ExecutionContext): AsyncStream[A]

  def toSeq(implicit  ec: ExecutionContext): Future[Seq[A]] =
    foldLeft(Seq[A]())(_ :+ _)

  def grouped(n: Int)(implicit ec: ExecutionContext): AsyncStream[AsyncStream[A]]
}

object AsyncStream {

  trait State[+A] {
    def isEmpty: Boolean
    def head: A
    def tail: AsyncStream[A]
  }

  object *:: {
    def unapply[A](xs: State[A]): Option[(A, AsyncStream[A])] =
      if (xs.isEmpty) None
      else Some((xs.head, xs.tail))
  }

  case object EmptyState extends State[Nothing] {
    override val isEmpty = true
    override def head = throw new NoSuchElementException
    override def tail = throw new NoSuchElementException
  }

  private case object Empty extends AsyncStream[Nothing] {

    override def state(implicit ec: ExecutionContext): Future[AsyncStream.State[Nothing]] = Future.successful(EmptyState)

    override def map[B](f: (Nothing) => B)(implicit ec: ExecutionContext): AsyncStream[B] = empty[B]
    override def flatMap[B](f: (Nothing) => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = empty[B]
    override def withFilter(p: (Nothing) => Boolean)(implicit ec: ExecutionContext): AsyncStream[Nothing] = Empty

    override def ++[B >: Nothing](that: => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = that

    override def take(n: Int)(implicit ec: ExecutionContext): AsyncStream[Nothing] = this

    override def drop(n: Int)(implicit ec: ExecutionContext): AsyncStream[Nothing]  = this

    override def foldLeft[B](z: B)(f: (B, Nothing) => B)(implicit ec: ExecutionContext): Future[B] =
      Future.successful(z)
    override def grouped(n: Int)(implicit ec: ExecutionContext): AsyncStream[Nothing] = this

    override def toString: String = "AsyncStream.Empty"
  }

  def empty[A]: AsyncStream[A] = Empty

  private class Cons[A](val first: A, next: => AsyncStream[A]) extends AsyncStream[A] { self =>

    private lazy val _tail = next

    override def state(implicit ec: ExecutionContext): Future[AsyncStream.State[A]] = Future.successful(new State[A] {
      override val isEmpty: Boolean = false
      override val head: A = first
      override def tail: AsyncStream[A] = _tail
    })

    override def map[B](f: (A) => B)(implicit ec: ExecutionContext): AsyncStream[B] = {
      new Cons(f(first), _tail map f)
    }
    override def flatMap[B](f: (A) => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = {
      f(first) ++ (_tail flatMap f)
    }

    override def ++[B >: A](that: => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = {
      new Cons(first, _tail ++ that)
    }

    override def withFilter(p: A => Boolean)(implicit ec: ExecutionContext): AsyncStream[A] = {
      if (p(first)) {
        new Cons(first, _tail.withFilter(p))
      } else {
        _tail.withFilter(p)
      }
    }

    override def take(n: Int)(implicit ec: ExecutionContext): AsyncStream[A] = {
      if (n <= 0) empty[A]
      else if (n == 1) new Cons(first, empty[A])
      else new Cons(first, _tail.take(n - 1))
    }

    override def drop(n: Int)(implicit ec: ExecutionContext): AsyncStream[A] = {
      if (n < 1) this else new FutureCons(Future(_tail.drop(n - 1)))
    }

    override def foldLeft[B](z: B)(f: (B, A) => B)(implicit ec: ExecutionContext): Future[B] = {

      @tailrec
      def foldLeftResolved(stream: AsyncStream[A], ac: B)(f: (B, A) => B): (AsyncStream[A], B) = stream match {
        case Empty => (Empty, ac)
        case s: Cons[A] => foldLeftResolved(s._tail, f(ac, s.first))(f)
        case s: FutureCons[A] => (s, ac)
      }

      val (s, ac) = foldLeftResolved(this, z)(f)
      s.foldLeft(ac)(f)
    }

    override def grouped(n: Int)(implicit ec: ExecutionContext): AsyncStream[AsyncStream[A]] = {
      require(n > 0)
      new Cons(take(n), drop(n).grouped(n))
    }

    override def toString = s"AsyncStream.Cons($first, ?)"
  }

  implicit class Syntax[A](tail: => AsyncStream[A]) {
    def *::(head: A): AsyncStream[A] = new Cons(head, tail)
  }

  private class FutureCons[A](fs: Future[AsyncStream[A]]) extends AsyncStream[A] {

    override def state(implicit ec: ExecutionContext): Future[AsyncStream.State[A]] = fs.flatMap(_.state)

    override def map[B](f: (A) => B)(implicit ec: ExecutionContext): AsyncStream[B] = {
      new FutureCons(fs.map(_ map f))
    }
    override def flatMap[B](f: (A) => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = {
      new FutureCons(fs.map(_ flatMap f))
    }

    override def withFilter(p: A => Boolean)(implicit ec: ExecutionContext): AsyncStream[A] = {
      new FutureCons(fs.map(_.withFilter(p)))
    }

    override def ++[B >: A](that: => AsyncStream[B])(implicit ec: ExecutionContext): AsyncStream[B] = {
      new FutureCons(fs.map(_ ++ that))
    }

    override def take(n: Int)(implicit ec: ExecutionContext): AsyncStream[A] = {
      new FutureCons(fs.map(_.take(n)))
    }

    override def drop(n: Int)(implicit ec: ExecutionContext): AsyncStream[A] = {
      new FutureCons(fs.map(_.drop(n)))
    }

    override def foldLeft[B](z: B)(f: (B, A) => B)(implicit ec: ExecutionContext): Future[B] = {
      fs.flatMap(_.foldLeft(z)(f))
    }

    override def grouped(n: Int)(implicit ec: ExecutionContext): AsyncStream[AsyncStream[A]] = {
      new FutureCons(fs.map(_.grouped(n)))
    }

    override def toString = s"AsyncStream.FutureCons(?, ?)"
  }

  def futureCons[A](fs: Future[AsyncStream[A]]): AsyncStream[A] = new FutureCons(fs)

  implicit class Conversions[A](val t: Traversable[A]) extends AnyVal {

    def toAsyncStream: AsyncStream[A] = if (t.isEmpty) empty[A] else t.head *:: t.tail.toAsyncStream
  }
}
