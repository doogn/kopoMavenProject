package com.ejkim

import org.apache.spark.sql.SparkSession

object Ex2_180330 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    // jdbc (java database connectivity) 연결
    val selloutData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    //selloutData.createOrReplaceTempView("selloutTable")
    //selloutData.show()


    // 데이터 유형 변경하기 (cast)
    spark.catalog.dropTempView("maindata")
    selloutData.createOrReplaceTempView("maindata")

    //selloutData.schema

    var data1 = spark.sql("select regionid, product,"
      + "yearweek, cast(qty as double) from maindata")

    data1.schema

    data1.show

   }

}
