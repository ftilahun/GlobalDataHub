package com.kainos.enstar.transformation.eclipse

import com.kainos.enstar.transformation.{ QueryTest, _ }
import com.kainos.enstar.transformation.sourcetype.Eclipse

class PolicyQuerySuite extends QuerySuite {

  override val sourceType = Eclipse

  override def testTags = List( tags.Policy )

  override def queryTestSets : List[QueryTestSet] = List(

    QueryTestSet(
      "Policy",
      "policy",
      "Policy.hql",
      Set(
        QueryTest(
          "mapping primary test data",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "PrimaryTestData.csv" )
        ),
        QueryTest(
          "testing all possible paths through the shareofwholepercent calculation",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "ShareOfWholePercentPaths.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "ShareOfWholePercentPaths.csv" ),
          order = List( "policystatuscode" )
        ),
        QueryTest(
          "testing data with null source fields",
          Set(
            CsvSourceData( "addr", "NullSourceFields.csv" ),
            CsvSourceData( "businesscode", "NullSourceFields.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "NullSourceFields.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "NullSourceFields.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "NullSourceFields.csv" )
        ),
        QueryTest(
          "testing objcode logic - unsatisfied left joins and objcode records with/without parent records",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "MultipleAndMissingObjCodes.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "MultipleAndMissingObjCodes.csv" ),
          order = List( "policynumber", "sublineofbusinesscode" )
        ),
        QueryTest(
          "testing businesscode logic - unsatisfied left joins and businesscode records with/without parent records",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "MultipleAndMissingBusinessCodes.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "MultipleAndMissingBusinessCodes.csv" ),
          order = List( "policynumber", "branchdescription" )
        ),
        QueryTest(
          "testing the population of the linkedmasterreference",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyendorsmnt", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyline", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyorg", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "PopulateLinkedMasterRef.csv" )
        ),
        QueryTest(
          "testing failed left joins on addr and policyendorsmnt",
          Set(
            CsvSourceData( "addr", "FailedLeftJoins.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "FailedLeftJoins.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "policy", "FailedLeftJoins.csv" )
        )
      ),
      Set(
        ReconciliationTest(
          "mapping primary test data",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PrimaryTestData.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PrimaryTestData.csv" ),
            CsvSourceData( "policyendorsmnt", "PrimaryTestData.csv" ),
            CsvSourceData( "policyline", "PrimaryTestData.csv" ),
            CsvSourceData( "policyorg", "PrimaryTestData.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          "policy",
          "policy/RecordCount.hql",
          "policy/RecordCount.hql"
        ),
        ReconciliationTest(
          "testing the population of the linkedmasterreference",
          Set(
            CsvSourceData( "addr", "PrimaryTestData.csv" ),
            CsvSourceData( "businesscode", "PrimaryTestData.csv" ),
            CsvSourceData( "objcode", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "org", "PrimaryTestData.csv" ),
            CsvSourceData( "policy", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyendorsmnt", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyline", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "policyorg", "PopulateLinkedMasterRef.csv" ),
            CsvSourceData( "role", "PrimaryTestData.csv" )
          ),
          "policy",
          "policy/RecordCount.hql",
          "policy/RecordCount.hql"
        )
      )
    )
  )
}