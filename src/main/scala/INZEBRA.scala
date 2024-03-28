import fonctions.{mail, read_write, schema_chemin_hdfs, utils}
object INZEBRA {



  def main(args: Array[String]): Unit = {

    val date                         = args(0)


    val rechargeInDetail              = read_write.readRechargeInDetail(schema_chemin_hdfs.table_read_in_detail, date)

    val rechargeDetaillee             = read_write.readRechargeDetaillee(schema_chemin_hdfs.table_read_detaillee, date)

    val rechargeInDetailFiltre        = utils.filtre_recharge_in_detail(rechargeInDetail)
    val rechargeDetailleeFiltre       = utils.filtre_recharge_detaillee(rechargeDetaillee)


    val inDetailAddRenameColumns      = utils.add_columns_and_rename_detail_in(rechargeInDetailFiltre)

    val detailleeAddRenameColumns     = utils.add_columns_and_rename_detaillee(rechargeDetailleeFiltre)
    val dfJoin                        = utils.reconciliationJOIN(inDetailAddRenameColumns, detailleeAddRenameColumns)

    val dfInZebra                     = utils.reconciliationInZebra(dfJoin)

    val dfAgg                         = utils.reconciliationAgregee(dfJoin, date)


    read_write.writeHiveInZebra(dfInZebra, schema_chemin_hdfs.header, schema_chemin_hdfs.table_write_reconciliation)

    read_write.writeHiveAgr(dfAgg, schema_chemin_hdfs.header, schema_chemin_hdfs.table_write_agr)



    read_write.writeMail(dfAgg, date)
    mail.sendMail(date)


    println("day = "+date)

  }

}
