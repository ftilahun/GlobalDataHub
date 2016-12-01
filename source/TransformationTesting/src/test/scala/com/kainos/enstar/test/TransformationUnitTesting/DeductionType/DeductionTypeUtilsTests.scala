package com.kainos.enstar.test.TransformationUnitTesting.DeductionType

import com.kainos.enstar.TransformationUnitTesting.DeductionTypeUtils
import org.scalatest.FlatSpec

/**
 * Created by adamf on 30/11/2016.
 */
class DeductionTypeUtilsTests extends FlatSpec {
  "lookupDeductionTypeMapping" should "generate a Row given 2 inputs" in {
    val column1 = "A"
    val column2 = "B"

    val row = DeductionTypeUtils.lookupDeductionTypeMapping( ( Array ( column1, column2 ) ) )

    assert( row.size == 3 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
  }

  "lookupDeductionTypeMapping" should "generate a Row given 3 inputs" in {
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    val row = DeductionTypeUtils.lookupDeductionTypeMapping( ( Array ( column1, column2, column3 ) ) )

    assert( row.size == 3 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == column3 )
  }

  "deductionTypeMapping" should "generate a Row given 3s inputs" in {
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    val row = DeductionTypeUtils.deductionTypeMapping( ( Array ( column1, column2, column3 ) ) )

    assert( row.size == 4 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == column3 )
  }

  "deductionTypeMapping" should "generate a Row given 4 inputs" in {
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"

    val row = DeductionTypeUtils.deductionTypeMapping( ( Array ( column1, column2, column3, column4 ) ) )

    assert( row.size == 4 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == column3 )
    assert( row.get( 3 ) == column4 )
  }

}
