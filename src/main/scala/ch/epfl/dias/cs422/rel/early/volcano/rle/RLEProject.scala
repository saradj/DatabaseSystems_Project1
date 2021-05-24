package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  *
  * Note that this in an RLE operator, so it receives [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]] and
  * produces [[ch.epfl.dias.cs422.helpers.rel.RelOperator.RLEentry]]
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEProject protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    //println("inside project open")
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    val next_tuple = input.next()
    if (next_tuple == NilRLEentry) {
      NilRLEentry
      //no more input
    } else {
      val start = next_tuple.get.startVID
      val len = next_tuple.get.length

      val value = (evaluator(next_tuple.get.value))
      Some(RLEentry(start,len, value)) //todo should it be like this
    }
    //
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}
