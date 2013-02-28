package scala.collection.workstealing
package test






object ParIterableLikeTest extends App {

  def copyToArray(size: Int) {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    val arr = new Array[Int](size)

    range.copyToArray(arr, 0, arr.length)
    for (i <- 0 until size) assert(arr(i) == i, (i, arr(i)))

    range.copyToArray(arr, size / 2, arr.length)
    for (i <- size / 2 until size) assert(arr(i) == (i - size / 2), (i, arr(i)))
  }

  def sum(size: Int) {
    val range: ParIterable[Int] = new ParRange(0 until size, Workstealing.DefaultConfig)
    assert(range.sum == (0 until size).sum)
  }

  copyToArray(15)
  copyToArray(150)
  copyToArray(1500)
  copyToArray(150000)
  copyToArray(150000000)

  sum(1)
  sum(15)
  sum(150)
  sum(1500)
  sum(1500000)
  sum(150000000)

}