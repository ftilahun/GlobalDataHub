package com.kainos.enstar.test.TransformationUnitTesting.TrustFund

import com.kainos.enstar.TransformationUnitTesting.TrustFundUtils
import org.scalatest.FlatSpec

/**
 * Created by caoimheb on 07/12/2016.
 */
class TrustFundUtilsTests extends FlatSpec {

  "lookupTrustFundMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"

    // Act
    val row = TrustFundUtils.lookupTrustFundMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "trustFundMapping" should "generate a Row" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"

    // Act
    val row = TrustFundUtils.trustFundMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )

  }

}
