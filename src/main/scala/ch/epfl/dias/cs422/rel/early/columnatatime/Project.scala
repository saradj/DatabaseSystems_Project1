package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  protected var storage: IndexedSeq[HomogeneousColumn] = _

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    *
    * FIXME
    */
  lazy val evals
    : IndexedSeq[IndexedSeq[HomogeneousColumn] => HomogeneousColumn] =
    projects.asScala
      .map(e => map(e, input.getRowType, isFilterCondition = false))
      .toIndexedSeq

  /**
    * @inheritdoc
    */
  def execute(): IndexedSeq[HomogeneousColumn] = {
    var res: IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()
    storage = input.execute()

    if (storage.isEmpty)
      return IndexedSeq()
    val lastCol = storage.lastOption.getOrElse(null)

    //val k = table_store.last
    //val transposed = table_store.transpose

    storage = storage.dropRight(1) //.filter(_.last.asInstanceOf[Boolean])//TODO shy is not instance of boolean??

    val k = (for (i <- evals.indices) yield {
      evals(i)(storage)
    })
    return k :+ lastCol
    /*
    for (i <- transposed.indices)
    {
      val evaluated = evals(transposed(i).dropRight(1))
      res = res :+ evaluated
    }
    res = res.transpose
    if(res.isEmpty)
      return res
    res:+ lastCol
  }*/
    res
  }

}
