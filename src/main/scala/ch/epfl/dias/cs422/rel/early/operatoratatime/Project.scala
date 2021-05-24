package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected var table_store: IndexedSeq[Column] = _
  protected var iter: Iterator[Column] = _
  protected var proj_elem: Column = _

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[Column] = {
    table_store = input.execute()

    if (table_store.isEmpty)
      return IndexedSeq()
    val lastCol = table_store.lastOption.getOrElse(null)

    //val k = table_store.last
    table_store = table_store.dropRight(1)
    val transposed = table_store.transpose

    val k = (for (i <- transposed.indices) yield {
      evaluator(transposed(i))
    })
    if(k.isEmpty)
      return k
     k.transpose :+ lastCol
  }
}
