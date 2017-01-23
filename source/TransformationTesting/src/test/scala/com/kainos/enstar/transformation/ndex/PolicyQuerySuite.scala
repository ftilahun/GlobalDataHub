package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation.sourcetype.{ Ndex, SourceType }
import com.kainos.enstar.transformation.{ CsvSourceData, _ }

/**
 * Created by neilri on 16/01/2017.
 */
class PolicyQuerySuite extends QuerySuite {

  override val sourceType : SourceType = Ndex

  override def testTags = List( tags.Policy )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Policy",
      "policy",
      "Policy.hql",
      Set(
        QueryTest(
          "mapping happy path data",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "with null inception date in layer",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "NullInceptionDate.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullInceptionDateInLayer.csv" )
        ),
        QueryTest(
          "with null expiry date in layer",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "NullExpiryDate.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullExpiryDateInLayer.csv" )
        ),
        QueryTest(
          "with null values in layer inception date, expiry date, filcode, unique market ref",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "VariousNullValues.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "VariousNullsInLayer.csv" )
        ),
        QueryTest(
          "with no link between line and lookup profit centre",
          Set(
            CsvSourceData( "line", "NullProfitCentreCode.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullProfitCentreCodeInLine.csv" )
        ),
        QueryTest(
          "with null block in line",
          Set(
            CsvSourceData( "line", "NullBlock.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullBlockInLine.csv" )
        ),
        QueryTest(
          "with line status not equal to C",
          Set(
            CsvSourceData( "line", "LineStatusNotEqualC.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "LineStatusNotEqualC.csv" )
        ),
        QueryTest(
          "with null business type in line",
          Set(
            CsvSourceData( "line", "NullBusinessType.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullBusinessTypeFromLine.csv" )
        ),
        QueryTest(
          "with TIUK profit centre",
          Set(
            CsvSourceData( "line", "TIUKProfitCentre.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "TIUKTest.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TIUKProfitCentre.csv" )
        ),
        QueryTest(
          "with TIE profit centre",
          Set(
            CsvSourceData( "line", "TIEProfitCentre.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "TIETest.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TIEProfitCentre.csv" )
        ),
        QueryTest(
          "with Syndicate 1301 Milan profit centre",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "SyndicateBranch.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "SyndicateBranch.csv" )
        ),
        QueryTest(
          "with null percentage fields in line",
          Set(
            CsvSourceData( "line", "NullPercentageFields.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullPercentageFields.csv" )
        ),
        QueryTest(
          "Organisation not able to be joined with Risk",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "OrganisationIdNotInRisk.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullDomicileCode.csv" )
        ),
        QueryTest(
          "Line left joined to line - linkedmasterref from parent line record",
          Set(
            CsvSourceData( "line", "LineLeftJoinToLine.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "LinkedMasterRefFromParentLine.csv" )
        ),
        QueryTest(
          "Line left joined to line - linkedmasterref null as no parent line record",
          Set(
            CsvSourceData( "line", "LineNotLeftJoinableToLine.csv" ),
            CsvSourceData( "layer", "PrimaryTestData.csv" ),
            CsvSourceData( "submission", "PrimaryTestData.csv" ),
            CsvSourceData( "risk", "PrimaryTestData.csv" ),
            CsvSourceData( "organisation", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_block", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_business_type", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_profit_centre", "PrimaryTestData.csv" ),
            CsvSourceData( "underwriting_block", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullLinkedMasterReference.csv" )
        )
      )
    )
  )
}
