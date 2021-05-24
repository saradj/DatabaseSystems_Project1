package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  NilRLEentry,
  NilTuple,
  RLEentry
}
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var r_keys: IndexedSeq[Int] = IndexedSeq()
  private var curr_right: RLEentry = _
  private var res_stored: Seq[RLEentry] = IndexedSeq()
  private var hash_table
    : Map[Int, List[RLEentry]] = Map() //implementing hashJoin
  var rlestart = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    rlestart = 0
    var counter: Int = 0
    val l_data: List[RLEentry] = left.iterator.toList
    hash_table = Map()
    curr_right = null
    res_stored = IndexedSeq()
    r_keys = getRightKeys
    for (left <- l_data) {
      counter += 1
      hash_table.get(get_tuple_hash_code(left, getLeftKeys)) match {
        case Some(list) =>
          hash_table += get_tuple_hash_code(left, getLeftKeys) -> (left :: list)
        case None =>
          hash_table += get_tuple_hash_code(left, getLeftKeys) -> List(left)
      }
    }
    right.open()
  }
  private def get_tuple_hash_code(tuple: RLEentry, keys: IndexedSeq[Int]): Int =
    keys.map(tuple.value(_)).hashCode()

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    @tailrec
    def rec_next(): Any = {
      right.next() match {
        case NilTuple =>
          NilTuple
        case r =>
          hash_table.get(get_tuple_hash_code(r.get, r_keys)) match {
            case Some(tuples) =>
              res_stored = tuples
              curr_right = r.get
            case _ =>
              rec_next()
          }
      }
    }

    res_stored match {
      case Seq() =>
        rec_next() match {
          case NilRLEentry => NilRLEentry
          case null        => NilRLEentry
          case _           => next()
        }

      case head +: tail =>
        val tmp = RLEentry(rlestart,
                           head.length * curr_right.length,
                           head.value ++ curr_right.value) //TODO see this
        rlestart += (head.length * curr_right.length).toInt //tried also with just updating rlestart += 1
        res_stored = tail
        Some(tmp)
    }

  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
