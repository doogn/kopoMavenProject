package com.ejkim

import org.apache.spark.sql.SparkSession

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
    val selloutData1 = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb1, "user" -> staticUser, "password" -> staticPw)).load
    val selloutData2 = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb2, "user" -> staticUser, "password" -> staticPw)).load

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

    // 파일 저장
    selloutData1.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("e:/testresult1.csv") // 저장파일명

    selloutData2.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("e:/testresult2.csv") // 저장파일명

    // 데이터베이스 주소 및 접속정보 설정
    var outputUrl = "jdbc:oracle:thin:@192.168.110.14:1522/XE"
    var outputUser = "ejkim"
    var outputPw = "ejkim22"

    // 데이터 저장
    var prop1 = new java.util.Properties
    prop1.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop1.setProperty("user", outputUser)
    prop1.setProperty("password", outputPw)
    var table1 = "inner_join_example"
    selloutData1.write.mode("overwrite").jdbc(outputUrl, table1, prop1)

    var prop2 = new java.util.Properties
    prop2.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop2.setProperty("user", outputUser)
    prop2.setProperty("password", outputPw)
    var table2 = "left_join_example"
    selloutData2.write.mode("overwrite").jdbc(outputUrl, table2, prop2)

  }
}
