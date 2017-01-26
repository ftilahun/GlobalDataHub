package com.kainos.enstar.transformation.udf

import org.apache.spark.sql.SQLContext

object TransformationUdfRegistry {

  val udfs = Map (
    "net_as_pct_of_gross" -> NetAsPctOfGross
  )

  def registerUdfs( sqlContext : SQLContext ) : Unit = {
    for ( ( udfName, udf ) <- udfs.toList ) {
      sqlContext.udf.register( udfName, udf )
    }
  }
}
