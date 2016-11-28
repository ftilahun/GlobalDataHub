package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by sionam on 23/11/2016.
 */
object LineOfBusinessUtils {

  def lookupBlockMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols(1) )
  }

  def underwritingBlockMapping( cols : Array[String] ) : Row = {
    cols match {
      case cols if cols.length == 2 => Row( cols( 0 ), cols( 1 ), null)
      case _ => Row( cols( 0 ), cols( 1 ), cols(2) )
    }

  }

  def lineOfBusinessMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 4 => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), null )
    case cols if cols.length == 3 => Row( cols( 0 ), cols( 1 ), cols( 2 ), null, null )
    case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols(4) )
  }

}
