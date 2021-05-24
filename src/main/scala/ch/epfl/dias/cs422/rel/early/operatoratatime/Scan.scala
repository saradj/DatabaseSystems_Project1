package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] =  {

    val ncols = table.getColumnStrategies.size()
    if(ncols==0)
      return IndexedSeq()

    implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)


    scannable match  {
      case cols: ColumnStore =>
        val k = cols.getRowCount
        if(cols.getRowCount ==0){
          return IndexedSeq()
        }else
        {
          val seq = for(i <- 0 until ncols)yield {
            {
              val k = cols.getColumn(i).toIndexedSeq
              k
            }
          }
          if(seq.isEmpty)
            return IndexedSeq()
          val res: IndexedSeq[Column] = seq :+ IndexedSeq.fill(seq(0).size)(true)
          res
        }
    }
  }
}
