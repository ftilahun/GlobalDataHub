package enstar.cdcprocessor.metrics

/**
 * Job Metrics Object
 */
case class JobMetrics(changeData: Long,
                      immatureChanges: Long,
                      matureChanges: Long,
                      processedChanges: Long,
                      history: Long,
                      allRecords: Long,
                      openAndClosedRecords: Long,
                      activeRecords: Long,
                      newHistory: Long) {
  override def toString: String =
    "==========================================================================" +
      "\nStatistics for this run" +
      "\n==========================================================================" +
      s"\nChanges read: $changeData" +
      s"\nRejected Changes: $immatureChanges" +
      s"\nAccepted Changes: $matureChanges" +
      s"\nDistinct Transactions: $processedChanges" +
      s"\nBase Records: $history" +
      s"\nTotal Records (unioned): $allRecords" +
      s"\n\t(After setting validTo): $openAndClosedRecords" +
      s"\nActive Records: $activeRecords" +
      s"\nInactive Records: $newHistory" +
      "\n=========================================================================="
}
