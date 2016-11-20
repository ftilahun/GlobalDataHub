package com.kainos.enstar.test

import com.kainos.enstar.testing.{ BranchUtils, SQLRunner, testing }
import org.scalatest.FlatSpec
/**
 * Created by terences on 20/11/2016.
 */
class Branch extends FlatSpec with Serializable {

  "et" should "go" in {

    val lookup_profit_centre = testing.populateDataFrame(
      getClass.getResource( "/lookup_profit_centre.csv" ).toString,
      getClass.getResource( "/lookup_profit_centre.avro" ).toString,
      BranchUtils.lookupProfitCentreMapping,
      TestContexts.sparkContext,
      TestContexts.sqlContext
    )

    val expectedBranch = testing.populateDataFrame(
      getClass.getResource( "/branch.csv" ).toString,
      getClass.getResource( "/branch.avro" ).toString,
      BranchUtils.lookupProfitCentreMapping,
      TestContexts.sparkContext,
      TestContexts.sqlContext
    )

    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val statement = SQLRunner.loadStatementFromResource( "Branch.hql" )

    val result = SQLRunner.runStatement( statement, TestContexts.sqlContext ).collect()

    result.foreach( println )

  }

}
