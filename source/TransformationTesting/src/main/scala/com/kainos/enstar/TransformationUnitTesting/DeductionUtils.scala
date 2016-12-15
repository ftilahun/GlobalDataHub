package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
 * Created by terences on 08/12/2016.
 */
object DeductionUtils {

  def lineMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      cols( 2 ),
      cols( 3 ),
      cols( 4 ),
      cols( 5 )
    )
  }

  def layerMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 )
    )
  }

  def layerDeductionMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      cols( 2 ).toInt,
      cols( 3 ),
      cols( 4 )
    )
  }

  def deductionMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ),
      cols( 1 ),
      cols( 2 ),
      cols( 3 ),
      cols( 4 ),
      cols( 5 ),
      cols( 6 ),
      cols( 7 ),
      cols( 8 ),
      cols( 9 ),
      cols( 10 ).toBoolean,
      cols( 11 ).toBoolean
    )
  }

  def getStuff (list: List[Row]): List[Row] = {

    var newList = ListBuffer[Row]()

    for( item <- list) {

      val currentIndex = list.indexOf(item)
      val currentSequenceNo = item.get(item.fieldIndex("sequence_no"))
      val currentDeductionId = item.get(item.fieldIndex("deduction_id"))
      val currentLineId = item.get(item.fieldIndex("line_id"))

      val previousItem = if (currentIndex == 0) null else list(currentIndex - 1)
      val previousNewItem = if (currentIndex == 0) null else newList(currentIndex - 1)
      val lastSequenceNo = if (currentIndex == 0) 0 else previousItem.get(previousItem.fieldIndex("sequence_no"))
      val lastNetAmount =
        if (currentIndex==0) item(item.fieldIndex("slip_income_amount")).toString.toDouble * (item(item.fieldIndex("reporting_line_pct")).toString.toDouble/100)
      else previousNewItem.get(3)
      val lastDeductionAmount = if (currentIndex==0) 0.0 else previousNewItem.get(4)

      val ded = item.get(item.fieldIndex("deduction_pct")).toString.toDouble

      val currentNet =  lastNetAmount.toString.toDouble - lastDeductionAmount.toString.toDouble
      val deductionAmount =
        if (currentSequenceNo == lastSequenceNo) lastDeductionAmount
        else currentNet.toString.toDouble * item.get(item.fieldIndex("deduction_pct")).toString.toDouble / 100

      //create new row and add to the list
      newList += Row(currentLineId,currentDeductionId,currentSequenceNo,currentNet.toString,deductionAmount.toString)
    }

    newList.toList
  }
}
