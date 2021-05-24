package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters.ListHasAsScala


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  var idxOfEntry= getRowType.getFieldList.asScala.map(e=> 0)
  /**
   * Helper function (you do not have to use it or implement it)
   * It's purpose is to show how to convert the [[scannable]] to a
   * specific [[Store]].
   *
   * @param rowId row number (startign from 0)
   * @return the row as a Tuple
   */
  private def getRow(rowId: Int): Tuple = {
    scannable match {
      case rleStore: RLEStore =>
        /**
         * For this project, it's safe to assume scannable will always
         * be a [[RLEStore]].
         */
        ???
    }
  }

  val compute: (Int)=> Tuple = {
    scannable match {

      case store: RLEStore =>{
        val rles = getRowType.getFieldList.asScala.map(e=> store.getRLEColumn(e.getIndex))
        (rowId: Int)=>{
          getRowType.getFieldList.asScala.map(e=>  {
            val colidx=e.getIndex
           while (rles(colidx)(idxOfEntry(colidx)).endVID<rowId) //while not at correct row in column
           idxOfEntry(colidx)+=1
          rles(colidx)(idxOfEntry(colidx)).value

          }).reduce((a,b)=>a++b)//concat for all columns
        }
      }


    }

  }

  var rowIndex = 0
  var rowCount = 0
  var storage:IndexedSeq[Tuple] = _
  var currTuples: IndexedSeq[Tuple] = IndexedSeq()
  var curCount = 0l
  var rleIndex = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    println("inside rle scan")
    storage = IndexedSeq[Tuple]()
    rowIndex=0;
    rowCount = scannable.getRowCount.toInt
val k = getRowType.getFieldCount

    rleIndex=0
    currTuples = IndexedSeq()


  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var result: Option[Tuple] = NilTuple


    if(rowIndex<rowCount ){
      result=Some(compute((rowIndex)))
      rowIndex+=1
    }
    result
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = rowIndex=0
}
