package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val leftCols: IndexedSeq[HomogeneousColumn] = left.execute()
    val rightCols: IndexedSeq[HomogeneousColumn] = right.execute()
    val leftKeys: IndexedSeq[Int] = getLeftKeys
    val hash_table_left
      : Map[IndexedSeq[RelOperator.Elem], List[RelOperator.Tuple]] = Map()
    val rightKeys: IndexedSeq[Int] = getRightKeys
    var result:IndexedSeq[RelOperator.Tuple] = IndexedSeq()

    val tmp_left: IndexedSeq[RelOperator.Tuple] = leftCols.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
    val tmp_right: IndexedSeq[RelOperator.Tuple] = rightCols.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
    for (row <- tmp_left) {
      val left_k: IndexedSeq[RelOperator.Elem] =
        leftKeys.map(leftKey => row(leftKey))
      if (hash_table_left.contains(left_k)) {
        val oldValue: List[RelOperator.Tuple] =
          hash_table_left.getOrElse(left_k, null)
        hash_table_left.update(left_k, oldValue :+ row)
      } else {
        hash_table_left.put(left_k, List(row))
      }

    }
    if (hash_table_left.isEmpty) IndexedSeq()
    else {
      for (row <- tmp_right) {
        val right_k: IndexedSeq[RelOperator.Elem] =
          rightKeys.map(rightKey => row(rightKey))
        if (hash_table_left.contains(right_k)) {
          result = result ++ hash_table_left
            .getOrElse(right_k, null)
            .map(t => (t ++ row))
        }
      }
      val r = result.toIndexedSeq.transpose.map(toHomogeneousColumn(_))
      if (r.isEmpty)
        return IndexedSeq()
      r :+ IndexedSeq.fill(r(0).size)(true)
    }

  }
}
