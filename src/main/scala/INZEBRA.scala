import fonctions.{mail, read_write, schema_chemin_hdfs, utils}
import org.apache.spark.sql.functions.col

object INZEBRA {



  def main(args: Array[String]): Unit = {



    val rechargeInDetail        = read_write.readParquet_in_zebra(false, schema_chemin_hdfs.chemin_in_detail, schema_chemin_hdfs.schemaRechargeInDetailDF)
    val rechargeDetaillee       = read_write.readParquet_in_zebra(false, schema_chemin_hdfs.chemin_detaillee, schema_chemin_hdfs.schemaRechargeDetaillee)

    val rechargeInDetailFiltre  = utils.filtre_recharge_in_detail(rechargeInDetail)
    val rechargeDetailleeFiltre = utils.filtre_recharge_detaillee(rechargeDetaillee)


    val inDetailAddRenameColumns = utils.add_columns_and_rename_detail_in(rechargeInDetailFiltre)
    val detailleeAddRenameColumns = utils.add_columns_and_rename_detaillee(rechargeDetailleeFiltre)

    val uniqueRowsWithoutSourceInDetail = inDetailAddRenameColumns.except(detailleeAddRenameColumns)
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



    /*inDetailAddRenameColumns.where(col("type_recharge")  === "Recharge Orange Money O&M").show()
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
