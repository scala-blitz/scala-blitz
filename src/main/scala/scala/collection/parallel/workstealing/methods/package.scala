package scala.collection.parallel.workstealing



import scala.reflect.macros._



package object methods {
  implicit def Optimiser(c: Context) = new Optimiser[c.type](c)
}