package fonctions

import constants._
import org.apache.spark.sql.{DataFrame, SaveMode}
object read_write {

  def readRechargeInDetail(table: String, jour: String): DataFrame = {
    spark.sql(
      s"""
        |SELECT * FROM $table
        |WHERE day = $jour
      """.stripMargin)
  }

  def readRechargeDetaillee(table: String, jour: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT * FROM $table
         |WHERE day = $jour
      """.stripMargin)
  }


  def writeHiveInZebra(dataFrame: DataFrame, header: Boolean, table: String) : Unit =  {
    dataFrame.repartition(1).write
      .format("hive")
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .option("header", header)
      .saveAsTable(table)
  }

  def writeHiveAgr(dataFrame: DataFrame, header: Boolean, table: String) : Unit =  {
    dataFrame.repartition(1).write
      .format("hive")
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .option("header", header)
      .saveAsTable(table)

  }

  def writeMail(dataFrame: DataFrame, partDay: String): Unit = {
    println("WRITETTTTTTTTTTTTTTTTTTTTTTTTTT")
    println("*" * 50 + "reconciliation_recharge : " + "*" * 50)
    dataFrame.write
      .format("com.crealytics.spark.excel")
      .option("sheetName", "in_zebra")
      .option("header", "true")
      .mode(SaveMode.Append)
      .save(s"/dlk/osn/refined/reconciliation_recharge_in_zebra/reconciliation_recharge_in_zebra.$partDay.xlsx")
  }

}
