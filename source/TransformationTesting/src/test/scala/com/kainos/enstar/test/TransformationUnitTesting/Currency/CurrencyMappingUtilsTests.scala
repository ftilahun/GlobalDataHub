package com.kainos.enstar.test.TransformationUnitTesting.Currency

import com.kainos.enstar.TransformationUnitTesting.CurrencyUtils
import org.scalatest.FlatSpec

/**
 * Created by adamf on 24/11/2016.
 */
class CurrencyMappingUtilsTests extends FlatSpec {
  "lookupCurrencyMapping" should "generate a Row given 1 input" in {
    val column1 = "1"

    val row = CurrencyUtils.lookupCurrencyMapping( ( column1 :: Nil ).toArray )

    assert( row.size == 2 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == null )
  }

  "lookupCurrencyMapping" should "generate a Row given 2 inputs" in {
    val column1 = "1"
    val column2 = "A"

    val row = CurrencyUtils.lookupCurrencyMapping( ( column1 :: column2 :: Nil ).toArray )

    assert( row.size == 2 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
  }

  "currencyMapping" should "generate a Row Given 2 inputs" in {
    val column1 = "1"
    val column2 = "£"

    val row = CurrencyUtils.currencyMapping( ( column1 :: column2 :: null :: Nil ).toArray )

    assert( row.size == 3 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == null )
  }

  "currencyMapping" should "generate a Row Given 3 inputs" in {
    val column1 = "1"
    val column2 = "£"
    val column3 = "A"

    val row = CurrencyUtils.currencyMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    assert( row.size == 3 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == column3 )
  }
}
