package org.scala.optimized.test


import scala.reflect.ClassTag



package par {
  class SimpleBuffer[@specialized(Int) T: ClassTag] {
    var narr = new Array[T](4)
    var narrpos = 0
    def pushback(x: T): this.type = {
      if (narrpos < narr.length) {
        narr(narrpos) = x
        narrpos += 1
        this
      } else {
        val newnarr = new Array[T](narr.length * 2)
        Array.copy(narr, 0, newnarr, 0, narr.length)
        narr = newnarr
        pushback(x)
      }
    }
  }
}
