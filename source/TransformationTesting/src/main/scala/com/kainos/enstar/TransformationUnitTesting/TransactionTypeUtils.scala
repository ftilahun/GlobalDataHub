package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

object TransactionTypeUtils {

  def lookupPremiumTypeMapping(cols : Array[String]): Row = {
    cols match {
      case cols if cols.length == 1 => Row(cols(0), null)
      case _ => Row(cols(0), cols(1))
    }
  }

  def transactionTypeMapping(cols : Array[String]): Row = {
    cols match {
      case cols if cols.length == 6 => Row(cols(0), cols(1), cols(2), cols(3), null, cols(4), cols(5).toBoolean)
      case _ => Row(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6).toBoolean)
    }
  }

}
