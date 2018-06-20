package com.ejkim

import org.apache.spark.sql.SparkSession

object load_save_db {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "customerdata"

    // jdbc (java database connectivity) 연결
    val selloutData1 = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutData1.createOrReplaceTempView("selloutTable1")

    // 데이터베이스 주소 및 접속정보 설정
    var outputUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장
    var prop1 = new java.util.Properties
    prop1.setProperty("driver", "org.postgresql.Driver")
    prop1.setProperty("user", outputUser)
    prop1.setProperty("password", outputPw)
    var table1 = "customerdata"
    selloutData1.write.mode("overwrite").jdbc(outputUrl, table1, prop1)

  }
}
