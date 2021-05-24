package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  var cur = 0l
  var curTuple: Option[Tuple] = NilTuple
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //println("inside decode open")
    input.open()
    cur = 0l
    curTuple=NilTuple
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {

    var res: Option[Tuple]  = NilTuple
    if(cur>0){
      res = curTuple
      cur-=1
    }
    else {
      val nextRle = input.next()
      if(nextRle!=NilRLEentry){
        res = Some(nextRle.get.value)
        curTuple=res
        cur = nextRle.get.length-1 //-1 TODO??
      }
    }
  res
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {

    cur = 0l
    curTuple = NilTuple
    input.close()
  }
}
