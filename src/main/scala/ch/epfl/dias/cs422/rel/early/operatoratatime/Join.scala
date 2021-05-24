package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {
    val left_columns: IndexedSeq[Column] = left.execute()
    val right_columns: IndexedSeq[Column] = right.execute()
    val leftKeys: IndexedSeq[Int] = getLeftKeys
    var hash_table_left
      : Map[IndexedSeq[RelOperator.Elem], List[RelOperator.Tuple]] = Map()
    val rightKeys: IndexedSeq[Int] = getRightKeys
    var resultt: IndexedSeq[RelOperator.Tuple] = IndexedSeq()

    val left_cols_tmp: IndexedSeq[RelOperator.Tuple] = left_columns.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
    val right_cols_tmp: IndexedSeq[RelOperator.Tuple] = right_columns.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))

    for (row <- left_cols_tmp) {
      val leftKeyValue: IndexedSeq[RelOperator.Elem] =
        leftKeys.map(leftKey => row(leftKey))
      if (hash_table_left.contains(leftKeyValue)) {
        val oldValue: List[RelOperator.Tuple] =
          hash_table_left.getOrElse(leftKeyValue, null)
        hash_table_left.update(leftKeyValue, oldValue :+ row)
      } else {
        hash_table_left.put(leftKeyValue, List(row))
      }

    }
    if (hash_table_left.isEmpty) IndexedSeq()
    else {
      for (row <- right_cols_tmp) {
        val rightKeyValue: IndexedSeq[RelOperator.Elem] =
          rightKeys.map(rightKey => row(rightKey))
        if (hash_table_left.contains(rightKeyValue)) {
          resultt = resultt ++ hash_table_left
            .getOrElse(rightKeyValue, null)
            .map(t => (t ++ row))
        }
      }
      val r = resultt.transpose
      if (r.isEmpty)
        return IndexedSeq()
      r :+ IndexedSeq.fill(r(0).size)(true)
    }

  }
}
