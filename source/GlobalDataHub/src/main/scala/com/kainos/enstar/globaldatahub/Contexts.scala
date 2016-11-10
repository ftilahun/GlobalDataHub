package com.kainos.enstar.globaldatahub

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

/**
  * Singleton for accessing spark/sql context.
  */
object Contexts {
  private val _sc : SparkContext = new SparkContext( new SparkConf() )
  private val _sqlC : SQLContext = new SQLContext( _sc )

  def sparkContext = _sc
  def sqlContext = _sqlC
}
