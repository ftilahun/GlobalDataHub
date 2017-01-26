package com.kainos.enstar.transformation.ndex

import com.kainos.enstar.transformation._

/**
 * Created by neilri on 11/01/2017.
 */
class AnalysisCodeSplitQuerySuite extends QuerySuite {

  val sourceType = sourcetype.Ndex

  override def testTags = List( tags.AnalysisCodeSplit )

  def queryTestSets = List(
    QueryTestSet(
      "AnalysisCodeSplit - RiskCode",
      "analysiscodesplit/riskcode",
      "AnalysisCodeSplitRiskCode.hql",
      Set(
        QueryTest(
          "mapping test one line row to one line_risk_code row",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_risk_code", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "analysiscodesplit", "PrimaryTestData.csv" ),
          order = List( "policynumber", "coveragereference" )
        ),
        QueryTest(
          "mapping test many line_risk_code rows to one line row",
          Set(
            CsvSourceData( "line", "ManyLineRiskToOneLine.csv" ),
            CsvSourceData( "line_risk_code", "ManyLineRiskToOneLine.csv" ),
            CsvSourceData( "lookup_risk_code", "ManyLineRiskToOneLine.csv" )
          ),
          CsvSourceData( "analysiscodesplit", "ManyLineRiskToOneLine.csv" ),
          order = List( "policynumber", "analysiscode" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "line_risk_code", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_risk_code", "PrimaryTestData.csv" )
          ),
          "analysiscodesplit",
          "AnalysisCodeSplit/RiskCodeRecordCount.hql",
          "AnalysisCodeSplit/RiskCodeRecordCount.hql"
        ),
        ReconciliationTest(
          "many line risks to one line for AnalysisCodeSplit RiskCode",
          Set(
            CsvSourceData( "line", "ManyLineRiskToOneLine.csv" ),
            CsvSourceData( "line_risk_code", "ManyLineRiskToOneLine.csv" ),
            CsvSourceData( "lookup_risk_code", "PrimaryTestData.csv" )
          ),
          "analysiscodesplit",
          "AnalysisCodeSplit/RiskCodeRecordCount.hql",
          "AnalysisCodeSplit/RiskCodeRecordCount.hql"
        )
      )
    ),
    QueryTestSet(
      "AnalysisCodeSplit - TrustFund",
      "analysiscodesplit/trustfund",
      "AnalysisCodeSplitTrustFund.hql",
      Set(
        QueryTest(
          "mapping test one line row to one layer_trust_fund row",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_trust_fund", "PrimaryTestData.csv" )
          ),
          CsvSourceData( "analysiscodesplit", "PrimaryTestData.csv" ),
          order = List( "coveragereference" )
        ),
        QueryTest(
          "mapping test many layer_trust_fund rows to one line row",
          Set(
            CsvSourceData( "line", "ManyTrustFundToOneLayer.csv" ),
            CsvSourceData( "layer_trust_fund", "ManyTrustFundToOneLayer.csv" ),
            CsvSourceData( "lookup_trust_fund", "ManyTrustFundToOneLayer.csv" )
          ),
          CsvSourceData( "analysiscodesplit", "ManyTrustFundToOneLayer.csv" ),
          order = List( "coveragereference" )
        )
      ),
      Set(
        ReconciliationTest(
          "Primary",
          Set(
            CsvSourceData( "line", "PrimaryTestData.csv" ),
            CsvSourceData( "layer_trust_fund", "PrimaryTestData.csv" ),
            CsvSourceData( "lookup_trust_fund", "PrimaryTestData.csv" )
          ),
          "analysiscodesplit",
          "AnalysisCodeSplit/TrustFundRecordCount.hql",
          "AnalysisCodeSplit/TrustFundRecordCount.hql"
        ),
        ReconciliationTest(
          "many trust funds to one layer for AnalysisCodeSplit TrustFund",
          Set(
            CsvSourceData( "line", "ManyTrustFundToOneLayer.csv" ),
            CsvSourceData( "layer_trust_fund", "ManyTrustFundToOneLayer.csv" ),
            CsvSourceData( "lookup_trust_fund", "ManyTrustFundToOneLayer.csv" )
          ),
          "analysiscodesplit",
          "AnalysisCodeSplit/TrustFundRecordCount.hql",
          "AnalysisCodeSplit/TrustFundRecordCount.hql"
        )
      )
    )
  )
}
