package com.kainos.enstar.test.TransformationUnitTesting.Branch

import com.kainos.enstar.transformation.TransformationTestBase

/**
 * Created by terences on 20/11/2016.
 */
class TransformationTests extends TransformationTestBase(
  "BranchTransformation test with Primary data",
  Map(
    "lookup_profit_centre" -> "/branch/input/lookup_profit_centre_PrimaryTestData.csv"
  ),
  "/branch/output/branch_PriamryTestData.csv",
  "Transformation/Branch.hql"
)