package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.kainos.enstar.TransformationUnitTesting.PolicyUtils
import org.scalatest.FlatSpec

/**
 * Created by caoimheb on 23/11/2016.
 */
class PolicyUtilsTests extends FlatSpec {

  "lineMapping" should "generate a Row given 9 inputs of correct type" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "C"
    val column3 = "D"
    val column4 = "4"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"
    val column8 = "2"

    val columns = Array( column0, column1, column2, column3,
      column4, column5, column6, column7, column8 )
    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 9 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == column8.toInt )

  }

  "lineMapping" should "generate a Row given 8 inputs of correct type with final value equal to null" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "C"
    val column3 = "D"
    val column4 = "4"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"

    val columns = Array( column0, column1, column2, column3,
      column4, column5, column6, column7 )

    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 9 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == null )

  }

  "lineMapping" should "generate a Row if risk_reference column is null" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = ""
    val column3 = "D"
    val column4 = "4"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"
    val column8 = "5"

    val columns = Array( column0, column1, column2, column3,
      column4, column5, column6, column7, column8 )

    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 9 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == column8.toInt )
  }

  "layerMapping" should "generate a Row given 6 inputs" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "C"
    val column3 = "D"
    val column4 = "E"
    val column5 = "F"

    // Act
    val row = PolicyUtils.layerMapping( ( column0 :: column1 :: column2 :: column3 :: column4 :: column5 :: Nil ).toArray )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )
  }

  "riskMapping" should "generate a Row given 5 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "4"
    val column4 = "A"

    // Act
    val row = PolicyUtils.riskMapping( Array( column0, column1, column2, column3, column4 ) )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ) == column3.toInt )
    assert( row.get( 4 ).equals( column4 ) )
  }

  "riskMapping" should "generate a Row given 4 inputs with final value null" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "4"

    // Act
    val row = PolicyUtils.riskMapping( Array( column0, column1, column2, column3 ) )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ) == column3.toInt )
    assert( row.get( 4 ) == null )
  }

  "submission" should "generate a Row given inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "4"

    // Act
    val row = PolicyUtils.submissionMapping( Array( column0, column1, column2, column3 ) )

    // Assert
    assert( row.size == 4 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ) == column3.toInt )
  }

  "organisation" should "generate a Row given 2 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "A"

    // Act
    val row = PolicyUtils.organisationMapping( Array( column0, column1 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )
  }

  "organisation" should "generate a Row given 1 inputs with final value null" in {

    // Arrange
    val column0 = "1"

    // Act
    val row = PolicyUtils.organisationMapping( Array( column0 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == null )
  }

  "lookup_business_type" should "generate a Row given inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "A"

    // Act
    val row = PolicyUtils.lookupBusinessTypeMapping( Array( column0, column1 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )
  }

  "lookupBlockMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"

    // Act
    val row = PolicyUtils.lookupBlockMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "lookupProfitCentreMapping" should "generate a Row" in {

    // Arrange
    val column0 = "0"
    val column1 = "B"

    // Act
    val row = PolicyUtils.lookupProfitCentreMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "underwritingBlockMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"

    // Act
    val row = PolicyUtils.underwritingBlockMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )

  }

  "lookupCountryMapping" should "generate a Row given 1 input" in {

    // Arrange
    val column1 = "A"

    // Act
    val row = PolicyUtils.lookupCountryMapping(Array(column1))

    // Assert
    assert( row.size == 1 )
    assert( row.get( 0 ).equals( column1 ) )

  }

  "lookupCountryMapping" should "generate a Row with the value being null" in {

    // Arrange
    val column1 = ""

    // Act
    val row = PolicyUtils.lookupCountryMapping(Array(column1))

    // Assert
    assert( row.size == 1 )
    assert( row.get( 0 ) == null )

  }

  "policyMapping" should "generate a Row given 28 inputs" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"
    val column3 = "D"
    val column4 = "E"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"
    val column8 = "I"
    val column9 = "J"
    val column10 = "K"
    val column11 = "L"
    val column12 = ""
    val column13 = "N"
    val column14 = "O"
    val column15 = "P"
    val column16 = "Q"
    val column17 = "S"
    val column18 = ""
    val column19 = "T"
    val column20 = "U"
    val column21 = "V"
    val column22 = "W"
    val column23 = "X"
    val column24 = "Y"
    val column25 = "Z"
    val column26 = "AA"
    val column27 = "27"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6,
      column7, column8, column9, column10, column11, column12, column13, column14, column15,
      column16, column17, column18, column19, column20, column21, column22, column23, column24,
      column25, column26, column27 )

    // Act
    val row = PolicyUtils.policyMapping( columns )

    // Assert
    assert( row.size == 28 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ).equals( column8 ) )
    assert( row.get( 9 ).equals( column9 ) )
    assert( row.get( 10 ).equals( column10 ) )
    assert( row.get( 11 ).equals( column11 ) )
    assert( row.get( 12 ) == null )
    assert( row.get( 13 ).equals( column13 ) )
    assert( row.get( 14 ).equals( column14 ) )
    assert( row.get( 15 ).equals( column15 ) )
    assert( row.get( 16 ).equals( column16 ) )
    assert( row.get( 17 ).equals( column17 ) )
    assert( row.get( 18 ) == null )
    assert( row.get( 19 ).equals( column19 ) )
    assert( row.get( 20 ).equals( column20 ) )
    assert( row.get( 21 ).equals( column21 ) )
    assert( row.get( 22 ).equals( column22 ) )
    assert( row.get( 23 ).equals( column23 ) )
    assert( row.get( 24 ).equals( column24 ) )
    assert( row.get( 25 ).equals( column25 ) )
    assert( row.get( 26 ).equals( column26 ) )
    assert( row.get( 27 ) == column27.toInt )

  }
}
