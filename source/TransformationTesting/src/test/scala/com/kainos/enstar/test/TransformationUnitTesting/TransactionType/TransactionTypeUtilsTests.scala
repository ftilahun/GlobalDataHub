package com.kainos.enstar.test.TransformationUnitTesting.TransactionType

import com.kainos.enstar.TransformationUnitTesting.TransactionTypeUtils
import org.scalatest.{ FlatSpec, Matchers }

class TransactionTypeUtilsTests extends FlatSpec with Matchers {

  "lookupDeductionTypeMapping" should "generate a Row given 2 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act
    val row = TransactionTypeUtils.lookupDeductionTypeMapping( Array( column1, column2 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( "A" ) )
    assert( row.get( 1 ).equals( "B" ) )

  }

  "transactionTypeMapping" should "generate a Row given 7 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"
    val column6 = "F"
    val column7 = "false"

    val rowArray = Array( column1, column2, column3, column4, column5, column6, column7 )

    // Act
    val row = TransactionTypeUtils.transactionTypeMapping( rowArray )

    // Assert
    assert( row.size == 7 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ).equals( column4 ) )
    assert( row.get( 4 ).equals( column5 ) )
    assert( row.get( 5 ).equals( column6 ) )
    assert( row.get( 6 ).equals( false ) )

  }

  "transactionTypeMapping" should "throw error when column 7 is not parsable as a boolean" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"
    val column6 = "F"
    val column7 = "k"

    val rowArray = Array( column1, column2, column3, column4, column5, column6, column7 )

    // Act
    a[IllegalArgumentException] should be thrownBy {
      TransactionTypeUtils.transactionTypeMapping( rowArray )
    }

  }

}
