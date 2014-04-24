package scala.collection.optimizer



import scala.language.experimental.macros
import scala.reflect.macros._
import scala.reflect.ClassTag
import scala.collection.par.generic._
import scala.collection.par._
import scala.collection.generic.CanBuildFrom
import Scheduler.{Node, Ref}


object Lists {

  trait Scope {
    implicit def ListOps[T](a: Optimized[List[T]]) = new Lists.Ops(a)
  }

  class Ops[T](val list: Optimized[List[T]]) {
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S): S = macro internal.ListMacros.aggregate[T,S]
   /* def accumulate[S](merger: Merger[T, S]): S = ???
    def foreach[U >: T](action: U => Unit): Unit = ???
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R): R = ???
    def reduce[U >: T](operator: (U, U) => U) = ???
    def fold[U >: T](z: => U)(op: (U, U) => U): U = ???
    def sum[U >: T](implicit num: Numeric[U]): U = ???
    def product[U >: T](implicit num: Numeric[U]): U = ???
    def min[U >: T](implicit ord: Ordering[U]): U = ???
    def max[U >: T](implicit ord: Ordering[U]): U = ???
    def find[U >: T](p: U => Boolean): Option[T] = ???
    def exists[U >: T](p: U => Boolean): Boolean = ???
    def forall[U >: T](p: U => Boolean): Boolean = ???
    def count[U >: T](p: U => Boolean): Int = ???
    def map[S, That](func: T => S)(implicit bf: CanBuildFrom[List[T], S, That]) = ???
    def filter[That](pred: T => Boolean)(implicit bf: CanBuildFrom[List[T], T, That]) = ???
    def flatMap[S, That](func: T => TraversableOnce[S])(implicit bf: CanBuildFrom[List[T], S, That]) = ???*/
  }
}

