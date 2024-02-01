package fonctions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object utils {

    def filtre_recharge_in_detail(df: DataFrame) : DataFrame =  {

        df.where(
              (col("canal_recharge") === "Ca Credit OM" && col("channel_name")
                .isin("Rech_OM_O&M", "Webservices Recharge(Orange Money)", "Rech_OM_Distri"))
                                                                            ||
              (col("canal_recharge") === "CA Wave")
                                                                            ||
              (col("canal_recharge") === "Ca IAH")
                                                                            ||
              (col("canal_recharge") === "Ca Seddo")
                                                                            ||
              (col("canal_recharge") === "Ca Cartes")

        )
    }


    def filtre_recharge_detaillee(df: DataFrame) : DataFrame =  {

      df.where(
              (col("type_recharge") === "M")
                                  ||
              (col("type_recharge") === "O")
                                  ||
              (col("type_recharge") === "D")
                                  ||
              (col("type_recharge") === "W")
                                  ||
              (col("type_recharge") === "V")
                                  ||
              (col("type_recharge") === "A")
                                  ||
              (col("type_recharge") === "E")
                                  ||
              (col("type_recharge") === "C")
      )
    }


    def add_columns_and_rename_detail_in(df: DataFrame) : DataFrame =  {

      df
        //.withColumnRenamed("day"             ,   "date")
        .withColumn("year"                       , substring(col("day"), 1, 4))
        .withColumn("month"                      , substring(col("day"), 5, 2))
        .withColumnRenamed("msisdn"          , "numero")
        .withColumn(          "type_recharge"   ,
             when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Rech_OM_O&M"                                                                             , lit("Recharge Orange Money O&M"))
            .when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Webservices Recharge(Orange Money)"                                                      , lit("Recharge Orange Money S2S"))
            .when(col("canal_recharge")  === "Ca Credit OM" && col("channel_name") === "Rech_OM_Distri"                                                                          , lit("Recharge Orange Money Distri"))
            .when(col("canal_recharge")  === "CA Wave"                                                                                                                                     , lit("Recharge Wave"))
            .when(col("canal_recharge")  === "Ca IAH"                                                                                                                                      , lit("Recharge IAH"))
            .when(col("canal_recharge")  === "Ca Seddo"     && col("channel_name") === "Corporate Recharge"                                                                      , lit("Seddo Corporate"))
            .when(col("canal_recharge")  === "Ca Seddo"     && col("channel_name").isin("MobileSelfService", "IT Subscription", "RMS Charging", "Webservices Recharge(C2S)"), lit("Recharge C2S"))
            .when(col("canal_recharge")  === "Ca Cartes"                                                                                                                                   , lit("Recharge Carte"))
            .otherwise(                                                                                                                                                                                lit( null))
        )
        .withColumn("montant"                                                                                                                                                              , col("montant").cast("double"))
        .drop(
          "channel_name"                ,
                    "channel_ca_recharge"         ,
                    "canal_recharge"              ,
                    "segment"                     ,
                    "marche"                      ,
                    "region"                      ,
                    "departement"                 ,
                    "commune_arrondissement"      ,
                    "zone_drv"                    ,
                    "zone_drvnew"                 ,
                    "zone_dvri"                   ,
                    "zone_dvrinew"
        )
        .select(
               "numero"                      ,
              "type_recharge"               ,
                    "formule"                     ,
                    "montant"                     ,
                    "year"                        ,
                    "month"                       ,
                     "day"
        )
    }


    def add_columns_and_rename_detaillee(df: DataFrame) : DataFrame =  {

      df
        //.withColumnRenamed("day"                                , "date")
        .withColumn("year"                       , substring(col("day"), 1, 4))
        .withColumn("month"                      , substring(col("day"), 5, 2))
        .withColumnRenamed("msisdn"                             , "numero")
        .withColumn("type_recharge"                                ,
          when(col("type_recharge")             === "M"            , lit("Recharge Orange Money O&M"))
            .when(col("type_recharge")          === "O"            , lit("Recharge Orange Money S2S"))
            .when(col("type_recharge")          === "D"            , lit("Recharge Orange Money Distri"))
            .when(col("type_recharge")          === "W"            , lit("Recharge Wave"))
            .when(col("type_recharge")          === "V"            , lit("Recharge IAH"))
            .when(col("type_recharge")          === "A"            , lit("Seddo Corporate"))
            .when(col("type_recharge")          === "E"            , lit("Recharge C2S"))
            .when(col("type_recharge")          === "C"            , lit("Recharge Carte"))
            .otherwise(                                                        lit( null))
        )
        .withColumn("formule"                                      ,
          regexp_replace(
            col("formule")                                         ,
                "^(HYBRIDE-|POSTPAID-|PREPAID-)"                    ,
                ""
          )
        )
        .withColumnRenamed("montant_recharge"                   ,  "montant")
        .drop(
          "date_recharge"                                          ,
          "heure_recharge"                                                   ,
          "canal_recharge"                                                   ,
          "rech_carte_verte"                                                 ,
          "type_recharge_desc"
        )
        .select(
          "numero"                                                     ,
          "type_recharge"                                             ,
                "formule"                                                    ,
                "montant"                                                    ,
                "year"                                                       ,
                "month"                                                      ,
                 "day"
        )
    }



  def unique_rows_with_source(dataFrame: DataFrame, valeur: String): DataFrame = {

      dataFrame.withColumn("source_in_zebra", lit(valeur)).select(
        "numero"          ,
        "type_recharge"  ,
              "formule"         ,
              "montant"         ,
              "source_in_zebra" ,
              "year"            ,
              "month"           ,
              "day"
      )
  }


  def agg_date_type_recharge(dataFrame: DataFrame, alias: String): DataFrame = {

    /*val dfConverted = dataFrame.withColumn("day", to_date(col("date"), "yyyyMMdd"))

    val formattedDF = dfConverted.withColumn("day", date_format(col("day"), "dd/MM/yyyy"))*/

    // Grouper les données et effectuer les agrégations
    val groupedDF = dataFrame.groupBy("year","month","day", "type_recharge")
      .agg(
        count("*").as(alias + "_cnt"),
        sum("montant").as(alias + "_mnt")
      )

    groupedDF.select("year", "month","day", "type_recharge", alias + "_cnt", alias + "_mnt")

  }


  def reconciliation_agregee(dfIn: DataFrame, dfZebra: DataFrame): DataFrame = {

    val dfInAgg = agg_date_type_recharge(dfIn, "in")
    val dfZebraAgg = agg_date_type_recharge(dfZebra, "ze")

    val dfJoin = dfInAgg.join(dfZebraAgg, Seq("year","month","day", "type_recharge"), "full")
    val dfFinal = dfJoin
      .withColumn("ecart_cnt",
        when(col("in_cnt") >= col("ze_cnt"), col("in_cnt") - col("ze_cnt")).otherwise(-(col("ze_cnt") - col("in_cnt")) ))
      .withColumn("ecart_mnt",
        when(col("in_mnt") >= col("ze_mnt"), col("in_mnt") - col("ze_mnt")).otherwise(-(col("ze_mnt") - col("in_mnt")) ))
    dfFinal.select("type_recharge", "ecart_cnt", "ecart_mnt", "in_cnt", "in_mnt", "ze_cnt", "ze_mnt", "year", "month", "day").orderBy("year", "month", "day", "type_recharge").na.fill(0)
  }



}
