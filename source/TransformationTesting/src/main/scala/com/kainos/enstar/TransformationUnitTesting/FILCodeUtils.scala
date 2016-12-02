package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

object FILCodeUtils {

  def lookupFilCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def filCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }

}
