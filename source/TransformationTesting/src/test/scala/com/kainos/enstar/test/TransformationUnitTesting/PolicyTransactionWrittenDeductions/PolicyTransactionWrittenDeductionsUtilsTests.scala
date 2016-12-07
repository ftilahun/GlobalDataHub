package com.kainos.enstar.test.TransformationUnitTesting.PolicyTransactionWrittenDeductions

import com.kainos.enstar.TransformationUnitTesting.PolicyTransactionDeductionsUtils
import org.scalatest.FlatSpec

/**
 * Created by sionam on 02/12/2016.
 */
class PolicyTransactionWrittenDeductionsUtilsTests extends FlatSpec {

  "line" should "generate a Row" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "AB"
    val column3 = "3"
    val column4 = "CD"
    val column5 = "EF"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineMapping( ( column0 :: column1 :: column2 :: column3 :: column4 :: column5 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ) == column3.toInt )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )
  }

  "line" should "generate a Row given 5 inputs with final value null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "AB"
    val column3 = "3"
    val column4 = "CD"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineMapping( ( column0 :: column1 :: column2 :: column3 :: column4 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ) == column3.toInt )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ) == null )
  }

  "line" should "generate a Row given 4 inputs with final two values of null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "AB"
    val column3 = "3"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineMapping( ( column0 :: column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ) == column3.toInt )
    assert( row.get( 4 ) == null )
    assert( row.get( 5 ) == null )
  }

  "line" should "generate a Row given 3 inputs with final three values of null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "AB"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
    assert( row.get( 5 ) == null )
  }

  "line" should "generate a Row given 2 inputs with final four values of null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
    assert( row.get( 5 ) == null )
  }

  "layer" should "generate a Row" in {

  // Arrange
  val column0 = "1"
  val column1 = "B"
  val column2 = "AA"
  val column3 = "1"

  // Act
  val row = PolicyTransactionDeductionsUtils.layerMapping( ( column0 :: column1 :: column2 :: column3 :: Nil ).toArray )

  // Assert
  assert( row.size == 4 )
  assert( row.get( 0 ) == column0.toInt )
  assert( row.get( 1 ).equals( column1 ) )
  assert( row.get( 2 ).equals( column2 ) )
  assert( row.get( 3 ).equals( column3 ) )
  }

  "layer" should "generate a Row given 3 inputs with final value null" in {

  // Arrange
  val column0 = "1"
  val column1 = "B"
  val column2 = "AA"

  // Act
  val row = PolicyTransactionDeductionsUtils.layerMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

  // Assert
  assert( row.size == 4 )
  assert( row.get( 0 ) == column0.toInt )
  assert( row.get( 1 ).equals( column1 ) )
  assert( row.get( 2 ).equals( column2 ) )
  assert( row.get( 3 ) == null )
  }

  "layer" should "generate a Row given 2 inputs with final two values of null" in {

  // Arrange
  val column0 = "1"
  val column1 = "B"

  // Act
  val row = PolicyTransactionDeductionsUtils.layerMapping( ( column0 :: column1 :: Nil ).toArray )

  // Assert
  assert( row.size == 4 )
  assert( row.get( 0 ) == column0.toInt )
  assert( row.get( 1 ).equals( column1 ) )
  assert( row.get( 2 ) == null )
  assert( row.get( 3 ) == null )
  }

  "layer" should "generate a Row given 1 input with final three values of null" in {

  // Arrange
  val column0 = "1"

  // Act
  val row = PolicyTransactionDeductionsUtils.layerMapping( ( column0 :: Nil ).toArray )

  // Assert
  assert( row.size == 4 )
  assert( row.get( 0 ) == column0.toInt )
  assert( row.get( 1 ) == null )
  assert( row.get( 2 ) == null )
  assert( row.get( 3 ) == null )
  }

  "layerDeduction" should "generate a Row given 2 inputs with final value null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"

    // Act
    val row = PolicyTransactionDeductionsUtils.layerDeductionMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == null )
  }

  "layerDeduction" should "generate a Row" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "AA"

    // Act
    val row = PolicyTransactionDeductionsUtils.layerDeductionMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )

  }

  "layerTrustFundMapping" should "generate a Row" in {

    // Arrange
    val column0 = "C"
    val column1 = "5"

    // Act
    val row = PolicyTransactionDeductionsUtils.layerTrustFundMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ) == column1.toInt )

  }

  "lineRiskCodeMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "1"

    // Act
    val row = PolicyTransactionDeductionsUtils.lineRiskCodeMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ) == column1.toInt )

  }

  "lookupDeductionTypeMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"

    // Act
    val row = PolicyTransactionDeductionsUtils.lookupDeductionTypeMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "lookupRiskCodeMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"

    // Act
    val row = PolicyTransactionDeductionsUtils.lookupRiskCodeMapping( ( column0 :: Nil ).toArray )

    // Assert
    assert( row.size == 1 )
    assert( row.get( 0 ).equals( column0 ) )

  }

  "lookupTrustFundMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"

    // Act
    val row = PolicyTransactionDeductionsUtils.lookupTrustFundMapping( ( column0 :: Nil ).toArray )

    // Assert
    assert( row.size == 1 )
    assert( row.get( 0 ).equals( column0 ) )

  }

  "settlementSchedule" should "generate a Row" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "AAA"
    val column4 = "BBB"

    // Act
    val row = PolicyTransactionDeductionsUtils.settlementScheduleMapping( ( column0 :: column1 :: column2 :: column3 :: column4 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
  }

  "settlementSchedule" should "generate a Row given 4 inputs with final value null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "AAA"

    // Act
    val row = PolicyTransactionDeductionsUtils.settlementScheduleMapping( ( column0 :: column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == null )
  }

  "settlementSchedule" should "generate a Row given 3 inputs with final two values of null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"

    // Act
    val row = PolicyTransactionDeductionsUtils.settlementScheduleMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
  }

  "settlementSchedule" should "generate a Row given 2 inputs with final three values of null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"

    // Act
    val row = PolicyTransactionDeductionsUtils.settlementScheduleMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
  }

  "policyTransactionDeductionsMapping" should "generate a Row given 18 inputs" in {

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
    val column17 = "S"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6,
      column7, column8, column9, column10, column11, column12, column13, column14, column15,
      column16, column17 )

    // Act
    val row = PolicyTransactionDeductionsUtils.policyTransactionDeductionsMapping( columns )

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
