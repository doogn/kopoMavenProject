package com.ejkim

import org.apache.spark.sql.SparkSession;

object Example_Join2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_region_mst"

    // jdbc (java database connectivity) 연결
    val selloutData1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load
    val selloutData2= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutData1.createOrReplaceTempView("selloutTable1")
    selloutData2.createOrReplaceTempView("selloutTable2")

    // inner join
    spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
      "from selloutTable1 a " +
      "inner join selloutTable2 b " +
      "on a.regionid = b.regionid")

    // left join
    spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
      "from selloutTable1 a " +
      "left join selloutTable2 b " +
      "on a.regionid = b.regionid")




  }
