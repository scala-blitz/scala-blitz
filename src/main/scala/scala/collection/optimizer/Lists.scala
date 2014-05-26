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
    implicit def ListOps[T](a: Optimized[List[T]]) = new Lists.Ops(a.seq)
  }

  class Ops[T](val list: List[T]) extends AnyVal {
    def aggregate[S](z: S)(combop: (S, S) => S)(seqop: (S, T) => S): S = macro internal.ListMacros.aggregate[T,S]
    def foreach[U >: T](action: U => Unit): Unit = macro internal.ListMacros.foreach[T, U]
    def mapReduce[R](mapper: T => R)(reducer: (R, R) => R): R = ???
    def reduce[U >: T](combop: (U, U) => U): U = macro internal.ListMacros.reduce[T, U]
    def fold[U >: T](z: => U)(op: (U, U) => U): U = macro internal.ListMacros.fold[T, U]
    def sum[U >: T](implicit num: Numeric[U]): U = macro internal.ListMacros.sum[T, U]
    def product[U >: T](implicit num: Numeric[U]): U = macro internal.ListMacros.product[T, U]
    def min[U >: T](implicit ord: Ordering[U]): U = macro internal.ListMacros.min[T, U]
    def max[U >: T](implicit ord: Ordering[U]): U = macro internal.ListMacros.max[T, U]
    def find[U >: T](p: U => Boolean): Option[T] = macro internal.ListMacros.find[T, U]
    def exists[U >: T](p: U => Boolean): Boolean = macro internal.ListMacros.exists[T, U]
    def forall[U >: T](p: U => Boolean): Boolean = macro internal.ListMacros.forall[T, U]
    def count[U >: T](p: U => Boolean): Int = macro internal.ListMacros.count[T, U]
    def map[S,  U >: T](p: U => S): List[S] = macro internal.ListMacros.map[T, U, S]
    def filter[That](p: T => Boolean): List[T] = macro internal.ListMacros.filter[T, T]
    def flatMap[S,  U >: T](p: U => TraversableOnce[S]): List[S] = macro internal.ListMacros.flatMap[T, U, S]
  }
}

