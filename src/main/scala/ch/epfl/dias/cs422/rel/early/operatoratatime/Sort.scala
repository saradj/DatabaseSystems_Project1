package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected var storage: IndexedSeq[Column] = _
  protected var table_sorted: IndexedSeq[IndexedSeq[Any]] = _
  protected var table_tmp: IndexedSeq[IndexedSeq[Any]] = _
  protected var table_result: IndexedSeq[IndexedSeq[Any]] = _
  protected var begining: Int = 0
  protected var isDecendingBool: Boolean = _

  protected var table_size: Int = 0

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[Column] = {
    val init_table = input.execute()

    val storage = init_table.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
    table_size = storage.size
    table_sorted = storage.sortWith((elem1, elem2) =>
      sortComparator(elem1, elem2, collation))

      if (table_size > begining + fetch.getOrElse(Int.MaxValue)) {
        begining += offset.getOrElse(0)
        table_size = begining + fetch.getOrElse(Int.MaxValue)
      }

    table_result = IndexedSeq[Column]()
    for (i <- begining until table_size) {
      table_result = table_result :+ table_sorted(i)
    }
    val table_resulthomogenous = table_result.transpose
    if (table_result.isEmpty)
      return IndexedSeq()
    (table_resulthomogenous :+ IndexedSeq.fill(table_resulthomogenous(0).size)(
      true))
  }

  def sortComparator(el1: Tuple, el2: Tuple, coll: RelCollation): Boolean = {
    var el1_comp: Comparable[Any] = null
    var el2_comp: Comparable[Any] = null
    var next_coll: java.util.List[RelFieldCollation] = null
    next_coll = coll.getFieldCollations
    for (i <- 0 until next_coll.size()) {
      el1_comp = el1(next_coll.get(i).getFieldIndex)
        .asInstanceOf[Comparable[Any]]
      el2_comp = el2(next_coll.get(i).getFieldIndex)
        .asInstanceOf[Comparable[Any]]
      if (el1_comp.compareTo(el2_comp) != 0) {
        isDecendingBool = el1_comp.compareTo(el2_comp) > 0
        if (next_coll.get(i).getDirection.isDescending)
          return isDecendingBool
        else
          return !isDecendingBool
      }
    }
    false
  }
}
