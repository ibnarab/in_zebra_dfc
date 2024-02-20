package fonctions

import constants._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType
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

  def readParquet_in_zebra(header: Boolean, chemin: String, schema: StructType) : DataFrame =  {

    spark.read
      .schema(schema)
      .format("parquet")
      .option("header", s"$header")
      .load(s"$chemin")
  }


  def writeHiveInZebra(dataFrame: DataFrame, header: Boolean, chemin: String, table: String) : Unit =  {
    dataFrame.repartition(1).write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day") // Spécifier les colonnes de partition
      .option("header", header)
      .option("path", chemin)
      .saveAsTable(table)
  }

  def writeMail(dataFrame: DataFrame, partMonth: String): Unit = {
    println("WRITETTTTTTTTTTTTTTTTTTTTTTTTTT")
    println("*" * 50 + "reconciliation_recharge : " + "*" * 50)
    dataFrame.write
      .format("com.crealytics.spark.excel")
      .option("sheetName", "in_zebra")
      .option("header", "true")
      .mode("overwrite")
      .save(s"/dlk/osn/refined/reconciliation_recharge_in_zebra.$partMonth.xlsx")
  }

}
