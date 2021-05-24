package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected var sorted: IndexedSeq[IndexedSeq[Any]] = _
  protected var table_tmp: IndexedSeq[IndexedSeq[Any]] = _
  protected var result_seq: IndexedSeq[IndexedSeq[Any]] = _
  protected var begining: Int = 0
  protected var isDecendingBool: Boolean = _

  protected var t_size: Int = 0

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val init_table = input.execute()

    val storage = init_table.transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
    t_size = storage.size
    sorted = storage.sortWith((elem1, elem2) =>
      sortComparator(elem1, elem2, collation))

    if (t_size > begining + fetch.getOrElse(Int.MaxValue)) {
      begining += offset.getOrElse(0)
      t_size = begining + fetch.getOrElse(Int.MaxValue)
    }

    result_seq = IndexedSeq[Column]()
    for (i <- begining until t_size) {
      result_seq = result_seq :+ sorted(i)
    }
    val table_resulthomogenous =
      result_seq.transpose.map(toHomogeneousColumn(_))
    if (table_resulthomogenous.isEmpty)
      IndexedSeq()
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
