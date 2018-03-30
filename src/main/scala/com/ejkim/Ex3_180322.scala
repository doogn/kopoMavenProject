package com.ejkim

import org.apache.spark.sql.SparkSession

object Ex3_180322 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    // 파일설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.14:1522/XE"
    var staticUser = "ejkim"
      var staticPw = "ejkim22"
      var selloutDb = "KOPO_PRODUCT_VOLUME"

      // jdbc (java database connectivity) 연결
      val selloutDataFromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

      // 메모리 테이블 생성
      selloutDataFromOracle.createOrReplaceTempView("selloutTable")
      selloutDataFromOracle.show(1)
  }
}


