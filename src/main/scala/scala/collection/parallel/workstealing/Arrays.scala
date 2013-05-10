package scala.collection.parallel
package workstealing






object Arrays {

  trait Scope {
    implicit def arrayOps[T](a: Par[Array[T]]) = ???
    
    implicit def array2zippable[T](a: Par[Array[T]]) = ???
  }

  // array ops implementations

}