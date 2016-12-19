package com.kainos.enstar.TransformationUnitTesting
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.types.{ StructType, IntegerType, DecimalType, ArrayType, DataType }
import math.BigDecimal._
/**
 * Created by neilri (&terences) on 13/12/2016.
 */
object NetAsPctOfGross extends UserDefinedAggregateFunction {

  val grossValue = java.math.BigDecimal.valueOf( 100 )

  val DeductionPctType = new StructType().add( "ded_seq", IntegerType ).add( "ded_pc", DecimalType( 10, 2 ) )

  override def inputSchema : StructType = DeductionPctType

  override def bufferSchema : StructType = new StructType().add( "deductions", ArrayType( DeductionPctType ) )

  override def dataType : DataType = DecimalType( 10, 2 )

  override def deterministic : Boolean = true

  override def initialize( buffer : MutableAggregationBuffer ) : Unit = {
    // initialise here
    buffer.update( 0, Seq[Row]() )
  }

  override def update( buffer : MutableAggregationBuffer, input : Row ) : Unit = {
    val deductions = buffer.getSeq[Row]( 0 )
    buffer.update( 0, deductions :+ input )
  }

  override def merge( buffer1 : MutableAggregationBuffer, buffer2 : Row ) : Unit = {
    val deductions1 = buffer1.getSeq[Row]( 0 )
    val deductions2 = buffer2.getSeq[Row]( 0 )
    buffer1.update( 0, deductions1 ++ deductions2 )
  }

  override def evaluate( buffer : Row ) : Any = {
    val c = BigDecimal( 100 )
    val deductions = buffer.getSeq[Row]( 0 )

    // Filter out null rows
    val filteredDeductions = deductions.filterNot( _.isNullAt( 0 ) )

    // group deductions by sequence number
    val groupedDeductions = filteredDeductions.groupBy( _.getInt( 0 ) )

    // sort grouped deductions by sequence number
    val sortedGroupedDeductions = groupedDeductions.toSeq.sortBy( _._1 )
    sortedGroupedDeductions.foldLeft( c ) {
      case ( netPc, ( _, rows ) ) =>
        // calculate all the deductions for the same sequence number using same net percentage as the base
        val groupDeductions = rows.map( row => netPc * ( row.getDecimal( 1 ) / c ) )
        val groupDeductionTotal = groupDeductions.sum
        // deduct total for this group from the
        netPc - groupDeductionTotal
    }
  }
}
