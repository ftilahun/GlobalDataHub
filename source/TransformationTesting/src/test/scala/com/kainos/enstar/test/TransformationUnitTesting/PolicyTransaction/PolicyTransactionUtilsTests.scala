package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransaction

import com.kainos.enstar.TransformationUnitTesting.PolicyTransactionUtils
import org.scalatest.FlatSpec

/**
 * Created by caoimheb on 28/11/2016.
 */
class PolicyTransactionUtilsTests extends FlatSpec {

  "layerMapping" should "generate a Row" in {

    // Arrange
    val column0 = "0"
    val column1 = "B"
    val column2 = "C"
    val column3 = "D"

    // Act
    val row = PolicyTransactionUtils.layerMapping( ( column0 :: column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 4 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )

  }

  "layerTrustFundMapping" should "generate a Row" in {

    // Arrange
    val column0 = "0"
    val column1 = "B"
    val column2 = "C"

    // Act
    val row = PolicyTransactionUtils.layerTrustFundMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )

  }

  "lineMapping" should "generate a Row" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "C"
    val column3 = "D"
    val column4 = "4"
    val column5 = "F"

    // Act
    val row = PolicyTransactionUtils.lineMapping( ( column0 :: column1 :: column2 :: column3 ::
      column4 :: column5 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )

  }

  "lineRiskCodeMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "2"

    // Act
    val row = PolicyTransactionUtils.lineRiskCodeMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ) == column2.toInt )

  }

  "lookupPremiumTypeMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"

    // Act
    val row = PolicyTransactionUtils.lookupPremiumTypeMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "settlementScheduleMapping" should "generate a Row" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "2"
    val column3 = "D"
    val column4 = "E"
    val column5 = "F"

    // Act
    val row = PolicyTransactionUtils.settlementScheduleMapping( ( column0 :: column1 :: column2 :: column3 ::
      column4 :: column5 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )

  }

  "policyTransactionMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"
    val column3 = "false"
    val column4 = "E"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"
    val column8 = "I"
    val column9 = "J"
    val column10 = "K"
    val column11 = "L"
    val column12 = "M"
    val column13 = "N"
    val column14 = "O"
    val column15 = "P"
    val column16 = "Q"
    val column17 = "R"

    // Act
    val row = PolicyTransactionUtils.policyTransactionMapping( ( column0 :: column1 :: column2 :: column3 ::
      column4 :: column5 :: column6 :: column7 :: column8 :: column9 :: column10 :: column11 :: column12 ::
      column13 :: column14 :: column15 :: column16 :: column17 :: Nil ).toArray )

    // Assert
    assert( row.size == 18 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3.toBoolean ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ).equals( column8 ) )
    assert( row.get( 9 ).equals( column9 ) )
    assert( row.get( 10 ).equals( column10 ) )
    assert( row.get( 11 ).equals( column11 ) )
    assert( row.get( 12 ).equals( column12 ) )
    assert( row.get( 13 ).equals( column13 ) )
    assert( row.get( 14 ).equals( column14 ) )
    assert( row.get( 15 ).equals( column15 ) )
    assert( row.get( 16 ).equals( column16 ) )
    assert( row.get( 17 ).equals( column17 ) )

  }

}
