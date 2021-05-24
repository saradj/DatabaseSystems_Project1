package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  var limit: Int = Integer.MAX_VALUE
  var idx: Int = 0
  var stored: IndexedSeq[Tuple] = IndexedSeq()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    limit = fetch.getOrElse(Integer.MAX_VALUE)
    if (limit == 0) {
      return
    }
    idx = 0
    input.open()

    var next_tuple = input.next()
    while (next_tuple != NilTuple) {
      stored :+= next_tuple.get
      next_tuple = input.next()
    }

    val collations = collation.getFieldCollations

    for (index <- (collation.getFieldCollations.size() - 1 to 0 by -1)) {
      stored = stored.sortWith((t1, t2) => {
        if (collations.get(index).shortString() == "DESC") {
          RelFieldCollation.compare(
            t1(collations.get(index).getFieldIndex).asInstanceOf[Comparable[_]],
            t2(collations.get(index).getFieldIndex).asInstanceOf[Comparable[_]],
            0) > 0
        } else {
          RelFieldCollation.compare(
            t1(collations.get(index).getFieldIndex).asInstanceOf[Comparable[_]],
            t2(collations.get(index).getFieldIndex).asInstanceOf[Comparable[_]],
            0) < 0
        }
      })
    }
    idx = offset.getOrElse(0)

  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (stored == null || idx >= stored.size || limit == 0) {
      return NilTuple
    }
    val next_tuple = stored(idx)
    idx += 1
    limit -= 1
    Some(next_tuple)
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

    input.close()
  }

}
