package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
                              left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
                            ) extends skeleton.Reconstruct[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  var curLeft: Option[RLEentry] = NilRLEentry
  var curRight: Option[RLEentry] = NilRLEentry

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    left.open()
    right.open()
    // make an initial call on both
    curLeft = left.next()
    curRight = right.next()

  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    while (curLeft.isDefined && curRight.isDefined) {
      val end = Math.min(curLeft.get.endVID, curRight.get.endVID)
      val start = Math.max(curLeft.get.startVID, curRight.get.startVID)
      if (end >= start) { // we found a match
        val v = curLeft.get.value ++ curRight.get.value

        if (curLeft.get.endVID.equals(end)) {  // curLeft is argmin
          curLeft = left.next()
        }
        if (curRight.get.endVID.equals(end)) { // curRight is argmin
          curRight = right.next()
        }
        return Some(RLEentry(startVID = start, length = end - start + 1, value = v))
      }
      // if an intersection is not found; the slower one needs to catch up
      if (curLeft.get.endVID.equals(end)) {
        curLeft = left.next()
      }
      if (curRight.get.endVID.equals(end)) {
        curRight = right.next()
      }
    }
    NilRLEentry  // one of the two is exhausted: stop recon
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
