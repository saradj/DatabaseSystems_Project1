package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  protected var iter : Iterator[Column] = _
  protected var agg_iter : Int = 0
  protected var index : Int = 0
  protected var elem : Column = _
  protected var elem_2 : Tuple = _

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {

    var datal = input.execute().toIndexedSeq
   var data: IndexedSeq[Column] = datal.map(unwrap(_).toIndexedSeq)
    if (data.isEmpty && groupSet.cardinality() == 0) {
     val f:IndexedSeq[HomogeneousColumn]= (for (call <- aggCalls) yield {
        IndexedSeq(call.emptyValue)
      })
       f:+IndexedSeq(true)
    } else if (data.isEmpty) {
      IndexedSeq()
    } else if (groupSet.cardinality() > 0) {
      val groupsIndexes = for (i <- (0 until groupSet.length()) if groupSet.get(i)) yield i
      var rows:IndexedSeq[Tuple] = getRowsFromCols(data)
      rows = rows.filter(e=> e.last.asInstanceOf[Boolean])
      val groupedBy: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] = rows.groupBy(tuple => groupsIndexes.map { i => tuple(i) })

      val processed = groupedBy.map { case (k: IndexedSeq[Any], tuples: IndexedSeq[Tuple]) =>
        (k, k ++ (for (call <- aggCalls) yield {
          tuples.init.foldLeft(call.getArgument(tuples.last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
        }))
      }.values.toIndexedSeq

      val columns = getColsFromRows(processed).map(toHomogeneousColumn(_))
      columns:+IndexedSeq.fill(processed.size)(true)

    } else {

      aggrGroup(data).map(toHomogeneousColumn(_))
    }
  }
  implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)
  def getTuple(data: IndexedSeq[Column], i: Int): Tuple ={
    data.flatMap { col: Column => col(i) } //TODO added drop check??
  }

  def aggrGroup(datan: IndexedSeq[Column]): IndexedSeq[Column] = {
    val aggregates = for (call <- aggCalls) yield {
      val data = getColsFromRows(getRowsFromCols(datan).filter(_.last.asInstanceOf[Boolean]))
      if(data.isEmpty)
        return IndexedSeq():+IndexedSeq(true)//need it!!
      val z = call.getArgument(getTuple(data, data(0).length - 1))
      (0 until data(0).length - 1).foldLeft(z)((acc, tupleidx) => call.reduce(acc, call.getArgument(getTuple(data, tupleidx))))
    }

    var res = aggregates.map(a => IndexedSeq(a))
    res :+ IndexedSeq.fill(res(0).size)(true)
  }
  def getRowsFromCols(inputData: IndexedSeq[Column]): IndexedSeq[IndexedSeq[Elem]] = {
    if(inputData.isEmpty) {
      return IndexedSeq()
    }
    for (row <- (0 until inputData(0).length)) yield ((0 until inputData.length).map(col => inputData(col)(row))).flatten
  }//.filter(e=> e.last.asInstanceOf[Boolean])

  def getColsFromRows(inputData: IndexedSeq[Tuple]): IndexedSeq[IndexedSeq[Elem]] = {
    if(inputData.isEmpty)
      return IndexedSeq()
    for (col <- (0 until inputData(0).length)) yield ((0 until inputData.length).map(row => inputData(row)(col))).flatten
  }
}
