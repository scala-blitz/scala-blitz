package scala.collection.workstealing






package benchmark {

  object MathUtils {
    def taylor(x: Int): Double = {
      var i = 0
      var num = 1
      var denom = 1
      var sum: Double = 1
      while (i < 10) {
        i += 1
        num *= x
        denom *= i
        sum += num.toDouble / denom
      }
      sum
    }
  }

}

