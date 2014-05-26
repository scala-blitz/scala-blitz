package scala.collection.optimizer


class Optimized[+Repr](val seq: Repr) extends AnyVal {
  override def toString = "Optimized(%s)".format(seq)
}

object Optimized {

  class OptimizedOps[+Repr](val underlying: Repr) extends AnyVal {
    override def toString = "OptimizedOps(%s)".format(underlying)
    def opt = new Optimized(underlying)
  }

}


