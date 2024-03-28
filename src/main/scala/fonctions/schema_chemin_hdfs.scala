package fonctions


object schema_chemin_hdfs {

  val table_read_in_detail = "refined_recharge.recharge_in_detail"
  val table_read_detaillee = "refined_recharge.recharge_detaillee"


  val chemin_write_in_detail = "/warehouse/tablespace/external/hive/dfc_temp.db/reconciliation_recharge_in_zebra"
  val chemin_write_detaillee = "/warehouse/tablespace/external/hive/dfc_temp.db/recharge_in_zebra_agr"

  val table_write_reconciliation = "dfc_temp.reconciliation_recharge_in_zebra"
  val table_write_agr = "dfc_temp.recharge_in_zebra_agr"

  val header = true



}
