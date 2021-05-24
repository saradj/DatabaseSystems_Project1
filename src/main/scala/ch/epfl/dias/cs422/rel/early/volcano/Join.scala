package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private val r_keys: IndexedSeq[Int] = getRightKeys
  private var curr_right: Tuple = IndexedSeq()
  private var res_stored: Seq[Tuple] = IndexedSeq()
  private var hash_table: Map[Int, List[Tuple]] = Map() //implementing hashJoin

  private def get_tuple_hash_code(tuple: Tuple, keys: IndexedSeq[Int]): Int =
    keys.map(tuple(_)).hashCode()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    var counter: Int = 0
    val l_data: List[Tuple] = left.iterator.toList
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

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
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
          case NilTuple => NilTuple
          case null     => NilTuple
          case _        => next()
        }

      case Seq(head, tail @ _*) =>
        val tmp = head ++ curr_right
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
