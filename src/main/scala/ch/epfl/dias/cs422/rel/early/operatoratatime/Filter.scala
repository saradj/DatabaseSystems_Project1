package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
                         input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                         condition: RexNode
                       ) extends skeleton.Filter[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }
  protected var iter : Iterator[Column] =_
  protected var table_storage : IndexedSeq[Column] = _
  protected var filter_elem : Column = _
  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[Column] = {
    var res : IndexedSeq[Column] = IndexedSeq[Column]()
    table_storage = input.toIndexedSeq

    table_storage = table_storage.transpose
    for(i <- table_storage.indices)
    {
      if(predicate(table_storage(i).dropRight(1)))
      {
        res = res :+ table_storage(i)
      }else
        res = res :+ (table_storage(i).dropRight(1).appended(false))
    }

    res.transpose
  }

}
