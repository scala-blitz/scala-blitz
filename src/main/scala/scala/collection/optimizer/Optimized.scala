package scala.collection.optimizer



class Optimized[+Repr](val seq: Repr) {
  override def toString = "Optimized(%s)".format(seq)
}

