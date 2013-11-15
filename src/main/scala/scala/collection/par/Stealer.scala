package scala.collection.par



trait Stealer[@specialized +T] {

  def next(): T

  def hasNext: Boolean

  /** Stealer state can be `AvailableOrOwned`, `Completed` or `StolenOrExpanded`.
   *  
   *  Stealer state can change from available to either completed or stolen,
   *  but cannot change once it is completed or stolen.
   */
  def state: Stealer.State

  /** A shortcut method that checks if the stealer is in the available state.
   */
  def isAvailable = state == Stealer.AvailableOrOwned

  /** Commits to processing a block of elements by using `next` and `hasNext`.
   *
   *  Once `hasNext` has returned `false`, `advance` can be called again.
   *
   *  @param step   approximate number of elements to commit to processing
   *  @return       `-1` if the stealer is not in the `AvailableOrOwned` state, otherwise
   *                an approximation on the number of elements that calls to `next` will return.
   */
  def advance(step: Int): Int

  /** Attempts to push the stealer to the `Completed` state.
   *  Only changes the state if the stealer was in the `AvailableOrOwned` state, otherwise does nothing.
   *
   *  Note: after having called this operation, the node will either be stolen or completed.
   * 
   *  @return `true` if node ends in the `Completed` state, `false` if it ends in the `StolenOrExpanded` state
   */
  def markCompleted(): Boolean

  /** Attempts to push the stealer to the `Stolen` state.
   *  Only changes the state if the stealer was in the `AvailableOrOwned` state, otherwise does nothing.
   *
   *  Note: after having called this operation, the node will either be stolen or completed.
   * 
   *  @return `false` if node ends in the `Completed` state, `true` if it ends in the `StolenOrExpanded` state
   */
  def markStolen(): Boolean

  /** Can only be called on stealers that are in the stolen state.
   *
   *  It will return a pair of stealers traversing the remaining elements
   *  of the current stealer.
   */
  def split: (Stealer[T], Stealer[T])

  /** Returns an estimate on the number of remaining elements.
   */
  def elementsRemainingEstimate: Int

  /** Minimum stealing threshold above which this stealer can be stolen and split.
   *
   *  Note: splitting and stealing still work if there are less elements, but the
   *  scheduler will not split such stealers.
   */
  def minimumStealThreshold: Int = 1

  /** Attempts to cast this stealer into a precise stealer.
   */
  def asPrecise: PreciseStealer[T] = this.asInstanceOf[PreciseStealer[T]]

  /** Optional.
   */
  def duplicated: Stealer[T] = ???

}


object Stealer {

  trait State

  val Completed = new State {
    override def toString = "Completed"
  }

  val StolenOrExpanded = new State {
    override def toString = "StolenOrExpanded"
  }

  val AvailableOrOwned = new State {
    override def toString = "AvailableOrOwned"
  }

  class Empty[@specialized T] extends Stealer[T] {
    def advance(step: Int): Int = -1
    def elementsRemainingEstimate: Int = 0
    def hasNext: Boolean = false
    def next(): T = throw new IllegalStateException
    def markCompleted(): Boolean = true
    def markStolen(): Boolean = false
    def state = Completed
    def split = throw new IllegalStateException
    override def toString = s"Stealer.Empty"
    override def duplicated = this
  }

  class Single[@specialized T](val elem: T) extends Stealer[T] {
    @volatile var completed = false
    var traversed = false
    def advance(step: Int): Int = {
      completed = true
      1
    }
    def elementsRemainingEstimate: Int = if (completed) 0 else 1
    final def hasNext: Boolean = completed && !traversed
    def next(): T = if (hasNext) {
      traversed = true
      elem
    } else throw new IllegalStateException
    def markCompleted(): Boolean = {
      completed = true
      traversed = true
      true
    }
    def markStolen(): Boolean = throw new IllegalStateException
    def state = if (completed) Completed else AvailableOrOwned
    def split = throw new IllegalStateException
    override def duplicated = {
      val s = new Single(elem)
      s.completed = this.completed
      s.traversed = this.traversed
      s
    }
    override def toString = s"Stealer.Single($elem, completed: $completed, traversed: $traversed)"
  }

}

