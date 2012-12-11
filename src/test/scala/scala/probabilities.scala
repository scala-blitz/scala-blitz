package scala




object probabilities extends App {

  object hit1 {
    def p(l: Int, r: Int, s: Int, w: Int): Double = {
      val left = {
        if (s <= l) p(l - s, r, 2 * s, w)
        else if (l < s && s <= l + w) 1.0
        else 0.0
      }

      val right = {
        if (s <= r) p(l, r - s, 2 * s, w)
        else if (r < s && s <= r + w) 1.0
        else 0.0
      }

      0.5 * left + 0.5 * right
    }
  }

  object hit2 {
    def p(l: Int, r: Int, s: Int, w: Int, period: Int, maxstep: Int): Double = {
      val newperiod = (period + 1) % 2
      val newstep = math.min(maxstep, if (newperiod == 0) s * 2 else s)

      val left = {
        if (s <= l) p(l - s, r, newstep, w, newperiod, maxstep)
        else if (l < s && s <= l + w) 1.0
        else 0.0
      }

      val right = {
        if (s <= r) p(l, r - s, newstep, w, newperiod, maxstep)
        else if (r < s && s <= r + w) 1.0
        else 0.0
      }

      0.5 * left + 0.5 * right
    }
  }

  val total = 2048
  val skew = 0.12
  val left = (total * (1 - skew) / 2).toInt
  val right = (total * (1 - skew) / 2).toInt
  val w = (skew * total).toInt

  println(hit1.p(left, right, 1, w))
  println(hit2.p(left, right, 1, w, 0, 256))

}