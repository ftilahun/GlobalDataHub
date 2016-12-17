package enstar.cdcprocessor

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Provides spark and hive contexts to be shared by all test cases.
 */
case class Data(id: Int, value: String)

object TestContexts {

  private val _sc: SparkContext = new SparkContext(
    new SparkConf()
      .setMaster("local[1]")
      .setAppName(this.getClass.getSimpleName))
  _sc.setLogLevel("OFF")
  private val _sqlC: SQLContext = new SQLContext(_sc)

  def sparkContext: SparkContext = _sc
  def sqlContext: SQLContext = _sqlC

  def dummyData(numItems: Int): DataFrame = {
    val list = (1 to numItems).map { number =>
      Data(number, "value" + number)
    }
    TestContexts.sqlContext.createDataFrame(
      TestContexts.sparkContext.parallelize(list))
  }

  def changeDummyData(numItems: Int): DataFrame = {
    val operations = List("INSERT", "UPDATE", "DELETE", "BEFOREIMAGE")
    val transactions = List("ONE", "TWO", "THREE", "FOUR")
    val changeOperation = udf(
      (num: Int) => operations(num % operations.length))
    val changeTransacion = udf(
      (num: Int) => transactions(num % operations.length)
    )
    val dateTime = udf(() => "2016-07-12 11:12:32.111")
    val changeSeq = udf(
      (num: Int) => "20160712111232110000000000000000000" + num)
    val data = dummyData(numItems)
    data
      .withColumn("header__operation", changeOperation(data("id")))
      .withColumn("header__timeStamp", dateTime())
      .withColumn("header__changesequence", changeSeq(data("id")))
      .withColumn("header__transaction", changeTransacion(data("id")))
      .withColumn("header__id", data("id"))
  }

}
