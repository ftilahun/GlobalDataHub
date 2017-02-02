package enstar.cdctableprocessor

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Provides spark and hive contexts to be shared by all test cases.
 */
case class GeneratedData(id: Int, value: String)

object TestContexts {

  private val _sc: SparkContext = new SparkContext(
    new SparkConf()
      //speed!!!!
      .set("spark.sql.shuffle.partitions", "1")
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName))
  _sc.setLogLevel("OFF")
  private val _sqlC: HiveContext = new HiveContext(_sc)

  def sparkContext: SparkContext = _sc
  def sqlContext: HiveContext = _sqlC

  def dummyData(numItems: Int): DataFrame = {
    val list = (1 to numItems).map { number =>
      GeneratedData(number, "value" + number)
    }
    TestContexts.sqlContext.createDataFrame(
      TestContexts.sparkContext.parallelize(list))
  }

  def changeDummyData(numItems: Int): DataFrame = {
    val operations = List("I", "U", "D", "B")
    val transactions = List("ONE", "TWO", "THREE", "FOUR")
    val changeOperation = udf(
      (num: Int) => operations(num % operations.length))
    val changeTransaction = udf(
      (num: Int) => transactions(num % operations.length)
    )
    val toTime = udf(() => "9999-12-31 23:59:59.000")
    val dateTime = udf(() => "2016-07-12 11:12:32.111")
    val changeSeq = udf(
      (num: Int) => "20160712111232110000000000000000000" + num)
    val data = dummyData(numItems)
    data
      .withColumn("header__operation", changeOperation(data("id")))
      .withColumn("header__timeStamp", dateTime())
      .withColumn("header__changesequence", changeSeq(data("id")))
      .withColumn("header__transaction", changeTransaction(data("id")))
      .withColumn("header__id", data("id"))
      .withColumn("validfrom", dateTime())
      .withColumn("validto", toTime())
  }
}
