import fonctions.{mail, read_write, schema_chemin_hdfs, utils, constants}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}
object INZEBRA {



  def main(args: Array[String]): Unit = {

    val debut = args(0)
    val fin = args(1)





    val rechargeInDetail        = read_write.readRechargeInDetail(schema_chemin_hdfs.table_read_in_detail, debut, fin)
    val rechargeDetaillee       = read_write.readRechargeDetaillee(schema_chemin_hdfs.table_read_detaillee, debut, fin)

    val rechargeInDetailFiltre  = utils.filtre_recharge_in_detail(rechargeInDetail)
    val rechargeDetailleeFiltre = utils.filtre_recharge_detaillee(rechargeDetaillee)


    val inDetailAddRenameColumns = utils.add_columns_and_rename_detail_in(rechargeInDetailFiltre)
    val detailleeAddRenameColumns = utils.add_columns_and_rename_detaillee(rechargeDetailleeFiltre)
    val dfJoin = utils.reconciliationINZEBRA(inDetailAddRenameColumns, detailleeAddRenameColumns)

    val dfAgg = utils.reconciliationAgregee(dfJoin)


    /*println("-----------------Schema---------------------------")

    inDetailAddRenameColumns.printSchema()
    //rechargeInDetail.printSchema()
    detailleeAddRenameColumns.printSchema()
    dfJoin.printSchema()



    println("----------------show-----------------------------")

    inDetailAddRenameColumns.show(5, false)
    //rechargeInDetail.show(5, false)
    detailleeAddRenameColumns.show(5, false)*/

    /*dfJoin.printSchema()
    dfJoin.show(5, false)*/

    dfJoin
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .saveAsTable("dfc_temp.in_zebra_test")

    dfAgg
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .saveAsTable("dfc_temp.in_zebra_agg_test")


    println("debut = "+debut)
    println("fin = "+fin)




    /*val uniqueRowsWithoutSourceInDetail = inDetailAddRenameColumns.except(detailleeAddRenameColumns)
    val uniqueRowsWithoutSourceDetaillee = detailleeAddRenameColumns.except(inDetailAddRenameColumns)


    val uniqueRowsWithSourceInDetail = utils.unique_rows_with_source(uniqueRowsWithoutSourceInDetail, "IN")
    val uniqueRowsWithSourceDetaillee = utils.unique_rows_with_source(uniqueRowsWithoutSourceDetaillee, "ZEBRA")




    val reconciliationRecharge = uniqueRowsWithSourceInDetail.union(uniqueRowsWithSourceDetaillee)
    val reconciliationAggregee = utils.reconciliation_agregee(inDetailAddRenameColumns, detailleeAddRenameColumns)



    read_write.writeHiveInZebra(reconciliationRecharge, true, schema_chemin_hdfs.chemin_write_in_detail, schema_chemin_hdfs.table_write_in_detail)
    read_write.writeHiveInZebra(reconciliationAggregee, true, schema_chemin_hdfs.chemin_write_detaillee, schema_chemin_hdfs.table_write_detaillee)



    val partitionmonth = mail.partitionMonth(args(0))
    read_write.writeMail(reconciliationAggregee, partitionmonth)
    mail.sendMail(partitionmonth)



    inDetailAddRenameColumns.where(col("type_recharge")  === "Recharge Orange Money O&M").show()
    detailleeAddRenameColumns.where(col("type_recharge")  === "Recharge Orange Money O&M").show()


    uniqueRowsWithoutSourceInDetail.where(col("type_recharge")  === "Recharge Orange Money O&M").show()
    uniqueRowsWithoutSourceDetaillee.where(col("type_recharge")  === "Recharge Orange Money O&M").show()


    uniqueRowsWithSourceInDetail.where(col("type_recharge")  === "Recharge Orange Money O&M").show()
    uniqueRowsWithSourceDetaillee.where(col("type_recharge")  === "Recharge Orange Money O&M").show()

    inDetailAddRenameColumns.where(col("type_recharge")  === "Recharge Carte").show()
    detailleeAddRenameColumns.where(col("type_recharge")  === "Recharge Carte").show()


    uniqueRowsWithoutSourceInDetail.where(col("type_recharge")  === "Recharge Carte").show()
    uniqueRowsWithoutSourceDetaillee.where(col("type_recharge")  === "Recharge Carte").show()


    uniqueRowsWithSourceInDetail.where(col("type_recharge")  === "Recharge Carte").show()
    uniqueRowsWithSourceDetaillee.where(col("type_recharge")  === "Recharge Carte").show()*/



  }

}
