package com.kainos.enstar.transformation.tags

import org.scalatest.Tag

object Transformation extends Tag( "type.Transformation" )
object Reconciliation extends Tag( "type.Reconciliation" )

// source system tags
object Ndex extends Tag( "source.Ndex" )
object Genius extends Tag( "source.Genius" )

// ECM table tags
object AnalysisCodeSplit extends Tag( "table.AnalysisCodeSplit" )
object Branch extends Tag( "table.Branch" )
object Broker extends Tag( "table.Broker" )
object Currency extends Tag( "table.Currency" )
object Deduction extends Tag( "table.Deduction" )
object DeductionType extends Tag( "table.DeductionType" )
object FILCode extends Tag( "table.FILCode" )
object Geography extends Tag( "table.Geography" )
object Insured extends Tag( "table.Insured" )
object LegalEntity extends Tag( "table.LegalEntity" )
object LineOfBusiness extends Tag( "table.LineOfBusiness" )
object MethodOfPlacement extends Tag( "table.MethodOfPlacement" )
object Policy extends Tag( "table.Policy" )
object PolicyEventType extends Tag( "table.PolicyEventType" )
object PolicyStatus extends Tag( "table.PolicyStatus" )
object PolicyTransaction extends Tag( "table.PolicyTransaction" )
object RiskCode extends Tag( "table.RiskCode" )
object TransactionType extends Tag( "table.TransactionType" )
object TrustFund extends Tag( "table.TrustFund" )
object Underwriter extends Tag( "table.Underwriter" )
