package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {

    val ncols = table.getColumnStrategies.size()

    implicit def anyflattener[A](a: A): Iterable[A] = Some(a)

    scannable match {
      case cols: ColumnStore =>
        val k = cols.getRowCount
        if (cols.getRowCount == 0) {
          return IndexedSeq()
        } else {
          val seq = for (i <- 0 until ncols) yield {
            {
              val k = cols.getColumn(i.toInt)
              k
            }
          }
          if (ncols == 0)
            return IndexedSeq()
          val res: IndexedSeq[HomogeneousColumn] = seq :+ IndexedSeq.fill(
            seq(0).size)(true)
          res
        }
    }
  }
}
