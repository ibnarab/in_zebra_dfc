package fonctions

import constants._
import org.apache.spark.sql.{DataFrame, SaveMode}
object read_write {

  def readRechargeInDetail(table: String, debut: String, fin: String): DataFrame = {
    spark.sql(
      s"""
        |SELECT * FROM $table
        |WHERE day between'$debut' and '$fin'
      """.stripMargin)
  }

  def readRechargeDetaillee(table: String, debut: String, fin: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT * FROM $table
         |WHERE day between'$debut' and '$fin'
      """.stripMargin)
  }


  def writeHiveInZebra(dataFrame: DataFrame, header: Boolean, table: String) : Unit =  {
    dataFrame.repartition(1).write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .option("header", header)
      .saveAsTable(table)

  }

  def writeHiveAgr(dataFrame: DataFrame, header: Boolean, table: String) : Unit =  {
    dataFrame.repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", header)
      .saveAsTable(table)

  }

  def writeMail(dataFrame: DataFrame, partMonth: String): Unit = {
    println("WRITETTTTTTTTTTTTTTTTTTTTTTTTTT")
    println("*" * 50 + "reconciliation_recharge : " + "*" * 50)
    dataFrame.write
      .format("com.crealytics.spark.excel")
      .option("sheetName", "in_zebra")
      .option("header", "true")
      .mode("ignore")
      .save(s"/dlk/osn/refined/reconciliation_recharge_in_zebra.$partMonth.xlsx")
  }

}
