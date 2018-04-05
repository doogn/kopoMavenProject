package com.ejkim

import org.apache.spark.sql.SparkSession;

object Example_Join {

def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainData = "kopo_channel_seasonality_ex.csv"
  var subData = "kopo_product_mst.csv"

  //Dataframe
  var mainDataDf = spark.read.format("csv").option("header", "true").load(dataPath + mainData)
  var subDataDf = spark.read.format("csv").option("header", "true").load(dataPath + subData)

  mainDataDf.createOrReplaceTempView("mainTable")
  subDataDf.createOrReplaceTempView("subTable")

  spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty " +
    "from mainTable a " +
    "left join subTable b " +
    "on a.productgroup = b.productid")
  // spark.sql 문을 여러 행에 나눠 쓸 때는 각 행 오른쪽 끝에 space 둘 것 (띄어쓰기)

  }
}
