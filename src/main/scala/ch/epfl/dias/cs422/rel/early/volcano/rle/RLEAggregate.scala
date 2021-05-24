package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  var idx: Int = 0
  var in_seq: IndexedSeq[RLEentry] = IndexedSeq()
  var processed: IndexedSeq[RLEentry] = IndexedSeq()
  var rlestart = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    in_seq = input.iterator.toIndexedSeq
    rlestart=0
    processed = IndexedSeq()
    idx = 0
    val idx_groups: IndexedSeq[Int] =
      for (i <- (0 until groupSet.length())
           if groupSet.get(i)) yield i
    val map_grouped_by =
      in_seq.groupBy(t =>
        idx_groups.map {
          t.value(_)
      })
    if (in_seq.isEmpty && idx_groups.isEmpty) { //input empty and group class empty
      processed = IndexedSeq(
        RLEentry(rlestart, 1, aggCalls.map(e => e.emptyValue))
      )
      rlestart+=1
    } else if (in_seq.isEmpty) { // in_seq is empty => processed = empty sequence
      processed = IndexedSeq()
    } else if (idx_groups.nonEmpty) { //both non-empty
      val newtuples = map_grouped_by
        .map {
          case (cnt: IndexedSeq[Any], seq_tuples: IndexedSeq[RLEentry]) =>
            (cnt,
             cnt ++
               aggCalls.map { call =>
                 seq_tuples.init.foldLeft(
                   call.getArgument(seq_tuples.last.value, seq_tuples.last.length))((acc, tuple) =>
                   call.reduce(acc, call.getArgument(tuple.value, tuple.length)))
               })
        }
        .values
        .toIndexedSeq
      processed = newtuples.map(RLEentry(rlestart, 1, _))
      rlestart += 1

    }
    else { // if only idx_groups are empty
      val newtuples = IndexedSeq(for (agg_call <- aggCalls) yield {
        in_seq.init.foldLeft(
          agg_call
            .getArgument(in_seq.last.value, in_seq.last.length))((acc, tuple) =>
          agg_call.reduce(acc, agg_call.getArgument(tuple.value, tuple.length)))
      })
      processed = newtuples.map(RLEentry(rlestart, 1, _))
      rlestart += 1
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {

    var next_tuple: Option[RLEentry] = NilRLEentry
    if (idx < processed.size && idx >= 0) {
      //println(processed(idx).getClass.getName)
      next_tuple = Some(processed(idx).asInstanceOf[RLEentry]) //get the next aggregated tuple
      idx += 1
    }
    next_tuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    rlestart = 0
    processed = IndexedSeq()
    idx = 0
    in_seq = IndexedSeq()

  }
}
