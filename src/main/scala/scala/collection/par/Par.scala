package scala.collection.par



class Par[+Repr](val seq: Repr) {
  override def toString = "Par(%s)".format(seq)
}

