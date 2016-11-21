package com.kainos.enstar.TransformationUnitTesting.test

import org.scalatest.FlatSpec
import com.kainos.enstar.TransformationUnitTesting
import com.kainos.enstar.TransformationUnitTesting.BranchUtils

/**
 * Created by terences on 20/11/2016.
 */
class BranchUtilsTests extends FlatSpec {

  "lookupProfitCentreMapping" should "generate a Row" in {

    // Arrange
    val column1 = "1"
    val column2 = "B"

    // Act
    val row = BranchUtils.lookupProfitCentreMapping( ( column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column1.toInt )
    assert( row.get( 1 ).equals( column2 ) )

  }

  "branchMapping" should "generate a Row given 3 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = BranchUtils.branchMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 4 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ) == null )

  }

  "branchMapping" should "generate a Row given 4 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"

    // Act
    val row = BranchUtils.branchMapping( ( column1 :: column2 :: column3 :: column4 :: Nil ).toArray )

    // Assert
    assert( row.size == 4 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ).equals( column4 ) )

  }
}
