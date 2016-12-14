package com.kainos.enstar.test.TransformationUnitTesting.Policy

import com.kainos.enstar.TransformationUnitTesting.PolicyUtils
import org.scalatest.FlatSpec

class PolicyUtilsTests extends FlatSpec {

  "lineMapping" should "generate a Row given 14 inputs of correct type" in {

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
    val column9 = "I"
    val column10 = "J"
    val column11 = "K"
    val column12 = "L"
    val column13 = "M"

    val columns = Array(
      column0, column1, column2, column3, column4, column5, column6,
      column7, column8, column9, column10, column11, column12, column13
    )

    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 14 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == column8.toInt )
    assert( row.get( 9 ).equals( column9 ) )
    assert( row.get( 10 ).equals( column10 ) )
    assert( row.get( 11 ).equals( column11 ) )
    assert( row.get( 12 ).equals( column12 ) )
    assert( row.get( 13 ).equals( column13 ) )

  }

  "lineMapping" should "generate a Row given 14 inputs of correct type with final 6 values equal to null" in {

    // Arrange
    val column0 = "0"
    val column1 = "1"
    val column2 = "C"
    val column3 = "D"
    val column4 = "4"
    val column5 = "F"
    val column6 = "G"
    val column7 = "H"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6, column7 )

    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 14 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == null )
    assert( row.get( 9 ) == null )
    assert( row.get( 10 ) == null )
    assert( row.get( 11 ) == null )
    assert( row.get( 12 ) == null )
    assert( row.get( 13 ) == null )

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
    val column8 = "2"
    val column9 = "I"
    val column10 = "J"
    val column11 = "K"
    val column12 = "L"
    val column13 = "M"

    val columns = Array(
      column0, column1, column2, column3, column4, column5, column6,
      column7, column8, column9, column10, column11, column12, column13
    )

    // Act
    val row = PolicyUtils.lineMapping( columns )

    // Assert
    assert( row.size == 14 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ) == column4.toInt )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ) == column8.toInt )
    assert( row.get( 9 ).equals( column9 ) )
    assert( row.get( 10 ).equals( column10 ) )
    assert( row.get( 11 ).equals( column11 ) )
    assert( row.get( 12 ).equals( column12 ) )
    assert( row.get( 13 ).equals( column13 ) )

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
    val row = PolicyUtils.layerMapping( Array( column0, column1, column2, column3, column4, column5 ) )

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
    val row = PolicyUtils.lookupBlockMapping( Array( column0, column1 ) )

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
    val row = PolicyUtils.lookupProfitCentreMapping( Array( column0, column1 ) )

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
    val row = PolicyUtils.underwritingBlockMapping( Array( column0, column1, column2 ) )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )

  }

  "policyMapping" should "generate a Row given 34 inputs" in {

    // Arrange
    val columns = Array(
      "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
      "", "N", "O", "P", "Q", "S", "", "T", "U", "V", "W", "X",
      "Y", "Z", "AA", "27", "BB", "AB", "AC", "AD", "AE", "AD"
    )

    // Act
    val row = PolicyUtils.policyMapping( columns )

    // Assert
    assert( row.size == 34 )
    assert( row.get( 0 ).equals( columns( 0 ) ) )
    assert( row.get( 1 ).equals( columns( 1 ) ) )
    assert( row.get( 2 ).equals( columns( 2 ) ) )
    assert( row.get( 3 ).equals( columns( 3 ) ) )
    assert( row.get( 4 ).equals( columns( 4 ) ) )
    assert( row.get( 5 ).equals( columns( 5 ) ) )
    assert( row.get( 6 ).equals( columns( 6 ) ) )
    assert( row.get( 7 ).equals( columns( 7 ) ) )
    assert( row.get( 8 ).equals( columns( 8 ) ) )
    assert( row.get( 9 ).equals( columns( 9 ) ) )
    assert( row.get( 10 ).equals( columns( 10 ) ) )
    assert( row.get( 11 ).equals( columns( 11 ) ) )
    assert( row.get( 12 ) == null )
    assert( row.get( 13 ).equals( columns( 13 ) ) )
    assert( row.get( 14 ).equals( columns( 14 ) ) )
    assert( row.get( 15 ).equals( columns( 15 ) ) )
    assert( row.get( 16 ).equals( columns( 16 ) ) )
    assert( row.get( 17 ).equals( columns( 17 ) ) )
    assert( row.get( 18 ) == null )
    assert( row.get( 19 ).equals( columns( 19 ) ) )
    assert( row.get( 20 ).equals( columns( 20 ) ) )
    assert( row.get( 21 ).equals( columns( 21 ) ) )
    assert( row.get( 22 ).equals( columns( 22 ) ) )
    assert( row.get( 23 ).equals( columns( 23 ) ) )
    assert( row.get( 24 ).equals( columns( 24 ) ) )
    assert( row.get( 25 ).equals( columns( 25 ) ) )
    assert( row.get( 26 ).equals( columns( 26 ) ) )
    assert( row.get( 27 ) == columns( 27 ).toInt )
    assert( row.get( 28 ).equals( columns( 28 ) ) )
    assert( row.get( 29 ).equals( columns( 29 ) ) )
    assert( row.get( 30 ).equals( columns( 30 ) ) )
    assert( row.get( 31 ).equals( columns( 31 ) ) )
    assert( row.get( 32 ).equals( columns( 32 ) ) )
    assert( row.get( 33 ).equals( columns( 33 ) ) )

  }
}
