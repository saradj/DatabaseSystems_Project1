package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  var idx: Int = 0
  var in_seq: IndexedSeq[Tuple] = IndexedSeq()
  var processed: IndexedSeq[IndexedSeq[Any]] = IndexedSeq()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {

    in_seq = input.iterator.toIndexedSeq
    val idx_groups: IndexedSeq[Int] =
      for (i <- (0 until groupSet.length())
           if groupSet.get(i)) yield i
    val map_grouped_by: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] =
      in_seq.groupBy(t =>
        idx_groups.map {
          t(_)
        })
    if (in_seq.isEmpty && idx_groups.isEmpty) { //input empty and group class empty
      processed = IndexedSeq(
        aggCalls.map(_.emptyValue)
      )
    } else if (in_seq.isEmpty) { // in_seq is empty => processed = empty sequence
      processed = IndexedSeq()
    } else if (idx_groups.nonEmpty) { //both non-empty
      processed = map_grouped_by
        .map {
          case (cnt: IndexedSeq[Any], seq_tuples: IndexedSeq[Tuple]) =>
            (cnt,
              cnt ++
                aggCalls.map {call=>
                  seq_tuples.init.foldLeft(call.getArgument(seq_tuples.last))(
                    (acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
                })
        }
        .values
        .toIndexedSeq

    } else { // if only idx_groups are empty
      processed = IndexedSeq(for (agg_call <- aggCalls) yield {
        in_seq.init.foldLeft(agg_call.getArgument(in_seq.last))((acc, tuple) =>
          agg_call.reduce(acc, agg_call.getArgument(tuple)))
      })

    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var next_tuple: Option[Tuple] = NilTuple
    if (idx < processed.size && idx >= 0) {
      next_tuple = Some(processed(idx)) //get the next aggregated tuple
      idx += 1
    }
    next_tuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}

