package com.kainos.enstar.transformation.genius

import com.kainos.enstar.transformation._
import com.kainos.enstar.transformation.sourcetype.SourceType

/**
 * Created by terences on 19/01/2017.
 */
class PolicyQuerySuite extends QuerySuite {

  override val sourceType : SourceType = sourcetype.Genius

  override def testTags = List( tags.Policy )

  override def queryTestSets : List[QueryTestSet] = List(
    QueryTestSet(
      "Policy",
      "policy",
      "Policy.hql",
      Set(
        QueryTest(
          "primary test data",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "test broken join from ZUMA to ZUCO",
          Set(
            CsvSourceData( "ZUMADF00", "BrokenJoinZUMAtoZUCO.csv" ),
            CsvSourceData( "ZUCODF00", "BrokenJoinZUMAtoZUCO.csv" ),
            CsvSourceData( "ZUSFDF00", "BrokenJoinZUMAtoZUCO.csv" ),
            CsvSourceData( "ZUSKDF00", "BrokenJoinZUMAtoZUCO.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "BrokenJoinZUMAtoZUCO.csv" )
        ),
        QueryTest(
          "testing left join to branch",
          Set(
            CsvSourceData( "ZUMADF00", "TestLeftJoinForBranch.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForBranch.csv" )
        ),
        QueryTest(
          "testing left join to lineofbusiness",
          Set(
            CsvSourceData( "ZUMADF00", "TestLeftJoinForLineOfBusiness.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForLineOfBusiness.csv" )
        ),
        QueryTest(
          "left join for linkedmasterreference",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "TestLeftJoinForLinkedMasterReference.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForLinkedMasterReference.csv" )
        ),
        QueryTest(
          "testing left join for sublineofbusiness",
          Set(
            CsvSourceData( "ZUMADF00", "TestLeftJoinForSublineOfBusiness.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForSublineOfBusiness.csv" )
        ),
        QueryTest(
          "testing left join for methodofplacementcode link table",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "TestLeftJoinForLinkTableToMethodOfPlacementCode.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForMethodOfPlacementCode.csv" )
        ),
        QueryTest(
          "testing left join for methodofplacementcode",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "TestLeftJoinForMethodOfPlacementCode.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForMethodOfPlacementCode.csv" )
        ),
        QueryTest(
          "testing left join for percentage",
          Set(
            CsvSourceData( "ZUMADF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZUCODF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZUGPDF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZUSPDF00", "TestLeftJoinForPercentages.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForPercentages.csv" )
        ),
        QueryTest(
          "testing left join for Method of of placement description",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "TestLeftJoinForMethodOfPlacementDescription.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestLeftJoinForMethodOfPlacementDescription.csv" )
        ),
        QueryTest(
          "test null us statecode",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "TestNullUSStateCode.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestNullUSStateCode.csv" )
        ),
        QueryTest(
          "test missingGeographyEntries",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "TestMissingGeographyEntries.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "TestMissingGeographyEntries.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary test data",
          Set(
            CsvSourceData( "ZUMADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUCODF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSFDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSKDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHDPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZURIDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZPKPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUM0DF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUMYDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHCVDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGSDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUGPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZUSPDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNDXDF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZHAADF00", "PrimaryTestData.csv" ),
            CsvSourceData( "ZNNZDF00", "PrimaryTestData.csv" )
          ),
          "policy",
          "policy/RecordCount.hql",
          "Policy/RecordCount.hql"
        )
      )
    )
  )
}
