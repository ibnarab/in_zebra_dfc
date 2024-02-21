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
        .withColumn("year_in"                       , substring(col("day"), 1, 4))
        .withColumn("month_in"                      , substring(col("day"), 5, 2))
        .withColumnRenamed("day"                , "day_in")
        .withColumnRenamed("msisdn"          , "numero_in")
        .withColumnRenamed("formule"          , "formule_in")
        .withColumn(          "type_recharge_in"   ,
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
        .withColumn("montant_in"                                                                                                                                                              , col("montant").cast("double"))
        .withColumn("date_recharge_in"                                                                                                                                                        , col("datetime_event").cast("timestamp"))
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
                    "zone_dvrinew"                ,
                    "datetime_event"
        )
        .select(
           "date_recharge_in"               ,
          "numero_in"                      ,
                "type_recharge_in"               ,
                "formule_in"                     ,
                "montant_in"                     ,
                "year_in"                        ,
                "month_in"                       ,
                "day_in"
        )
    }


    def add_columns_and_rename_detaillee(df: DataFrame) : DataFrame =  {

      df
        //.withColumnRenamed("day"                                , "date")
        .withColumn("year_zebra"                       , substring(col("day"), 1, 4))
        .withColumn("month_zebra"                      , substring(col("day"), 5, 2))
        .withColumnRenamed("day"                      , "day_zebra")
        .withColumnRenamed("msisdn"                             , "numero_zebra")
        .withColumnRenamed("date_recharge"                              ,  "date_recharge_zebra")
        .withColumnRenamed("montant_recharge"                   ,  "montant_zebra")
        .withColumn("type_recharge_zebra"                                ,
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
        .withColumn("formule_zebra"                                      ,
          regexp_replace(
            col("formule")                                         ,
                "^(HYBRIDE-|POSTPAID-|PREPAID-)"                    ,
                ""
          )
        )
        .drop(
          "heure_recharge"                                         ,
          "canal_recharge"                                                   ,
          "rech_carte_verte"                                                 ,
          "type_recharge_desc"
        )
        .select(
          "date_recharge_zebra"                                               ,
          "numero_zebra"                                                     ,
                "type_recharge_zebra"                                              ,
                "formule_zebra"                                                    ,
                "montant_zebra"                                                    ,
                "year_zebra"                                                       ,
                "month_zebra"                                                      ,
                 "day_zebra"
        )
    }


  def reconciliationINZEBRA(df_in: DataFrame, df_zebra: DataFrame): DataFrame = {
    val df1 = df_in.join(df_zebra,
      df_in("date_recharge_in") === df_zebra("date_recharge_zebra") &&
      df_in("numero_in") === df_zebra("numero_zebra") &&
        df_in("type_recharge_in") === df_zebra("type_recharge_zebra"),
      "full")

    val df2 = df1
      .withColumn("date_recharge",
      when(col("date_recharge_in") === col("date_recharge_zebra"), col("date_recharge_in"))
        .otherwise(coalesce(col("date_recharge_in"), col("date_recharge_zebra"))))
      .withColumn("numero",
        when(col("numero_in") === col("numero_zebra"), col("numero_in"))
          .otherwise(coalesce(col("numero_in"), col("numero_zebra"))))
      .withColumn("type_recharge",
        when(col("type_recharge_in") === col("type_recharge_zebra"), col("type_recharge_in"))
          .otherwise(coalesce(col("type_recharge_in"), col("type_recharge_zebra"))))
      .withColumn("formule",
        when(col("formule_in") === col("formule_zebra"), col("formule_in"))
          .otherwise(coalesce(col("formule_in"), col("formule_zebra"))))
      .withColumn("year",
        when(col("year_in") === col("year_zebra"), col("year_in"))
          .otherwise(coalesce(col("year_in"), col("year_zebra"))))
      .withColumn("month",
        when(col("month_in") === col("month_zebra"), col("month_in"))
          .otherwise(coalesce(col("month_in"), col("month_zebra"))))
      .withColumn("day",
        when(col("day_in") === col("day_zebra"), col("day_in"))
          .otherwise(coalesce(col("day_in"), col("day_zebra"))))
      .withColumn("ecart",
        when(col("date_recharge_in") === col("date_recharge_zebra")
          &&
          col("numero_in") === col("numero_zebra")
                      &&
          col("type_recharge_in") === col("type_recharge_zebra")
            &&
          col("montant_in") =!= col("montant_zebra"),
              lit("mismatch"))
          .when(col("date_recharge_in").isNull
            &&
            col("numero_in").isNull
            &&
            col("type_recharge_in").isNull
            ,
            lit("missing_in"))
          .when(col("date_recharge_zebra").isNull
            &&
            col("numero_zebra").isNull
            &&
            col("type_recharge_zebra").isNull
            ,
            lit("missing_zebra"))
          .otherwise(null)
        )
      //.withColumn("year",   year(col("input")))
      .drop("date_recharge_in", "date_recharge_zebra", "numero_in",
        "numero_zebra", "type_recharge_in", "type_recharge_zebra", "formule_in", "formule_zebra", "year_in", "year_zebra",
      "month_in", "month_zebra", "day_in", "day_zebra")
      .na.drop("any", Seq("ecart"))
      .select("date_recharge", "numero", "type_recharge", "formule", "montant_in", "montant_zebra", "ecart")

    df2
      .withColumn("year", year(col("date_recharge")).cast("String"))
      .withColumn("month", format_string("%02d", month(col("date_recharge"))))
      .withColumn("day", date_format(col("date_recharge"), "yyyyMMdd").cast("String"))

  }



  def reconciliationAgregee(dataFrame: DataFrame) : DataFrame = {

    dataFrame
      .groupBy("type_recharge", "year", "month", "day")
      .agg(
        count(when(col("ecart") === "mismatch", true)).alias("nombre_mismatch") ,
        count(when(col("ecart") === "missing_in", true)).alias("nombre_missing_in") ,
        count(when(col("ecart") === "missing_zebra", true)).alias("nombre_missing_zebra"),
        sum(when(col("ecart").isin("mismatch", "missing_zebra"), col("montant_in")).otherwise(0)).alias("total_montant_in"),
        sum(when(col("ecart").isin("mismatch", "missing_in"), col("montant_zebra")).otherwise(0)).alias("total_montant_zebra")
      )
      .drop("numero", "formule", "montant_in", "montant_zebra", "ecart")
      .select("type_recharge", "total_montant_in", "total_montant_zebra", "nombre_mismatch", "nombre_missing_in", "nombre_missing_zebra", "year", "month", "day")
      .orderBy("day", "year", "month", "day")
  }



  /*def unique_rows_with_source(dataFrame: DataFrame, valeur: String): DataFrame = {

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
    val dfNull = dfJoin.na.fill(0)
    val dfFinal = dfNull
      .withColumn("ecart_cnt",
        when(col("in_cnt") >= col("ze_cnt"), col("in_cnt") - col("ze_cnt")).otherwise(-(col("ze_cnt") - col("in_cnt")) ))
      .withColumn("ecart_mnt",
        when(col("in_mnt") >= col("ze_mnt"), col("in_mnt") - col("ze_mnt")).otherwise(-(col("ze_mnt") - col("in_mnt")) ))
    dfFinal.select("type_recharge", "ecart_cnt", "ecart_mnt", "in_cnt", "in_mnt", "ze_cnt", "ze_mnt", "year", "month", "day").orderBy("year", "month", "day", "type_recharge")
  }*/



}
