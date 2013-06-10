package scala.collection.parallel






trait PreciseStealer[@specialized +T] extends Stealer[T] {

  def next(): T

  def hasNext: Boolean

  /** Commits to processing a block of elements by using `next` and `hasNext`.
   *
   *  Once `hasNext` has returned `false`, `advance` can be called again.
   *
   *  @param step   upper bound on the number of elements to commit to processing
   *  @return       `-1` if the stealer is not in the `AvailableOrOwned` state, otherwise
   *                an exact number of elements that calls to `next` will return.
   */
  def advance(step: Int): Int

  def split: (PreciseStealer[T], PreciseStealer[T])

  /** Splits this stealer into a pair of stealers that traverse its remaining elements.
   *  The first stealer will have exactly `leftSize` elements.
   *
   *  Note: stealer needs to be in the stolen state.
   */
  def psplit(leftSize: Int): (PreciseStealer[T], PreciseStealer[T])

  def elementsRemainingEstimate: Int = elementsRemaining

  /** An exact number of remaining elements in this stealer that its owner
   *  has not yet committed to.
   *
   *  @return an exact number of remaining elements
   */
  def elementsRemaining: Int

  /** A total number of elements that this stealer could commit to
   *  at the time of its creation.
   *
   *  @return total number of elements in the stealer
   */
  def totalElements: Int

  /** The number of elements already committed to by this stealer.
   *
   *  @return number of elements the stealer already committed to
   */
  def elementsCompleted = totalElements - elementsRemaining

}


object PreciseStealer {

}