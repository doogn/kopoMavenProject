package com.midTerm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object no2_3 {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    ////////////////////////////// 2번 문제 //////////////////////////////

    // oracle 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var salesDb = "KOPO_CHANNEL_SEASONALITY_NEW"

    // jdbc 연결하여 자료 로딩 => 데이터프레임에 저장
    val salesDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> salesDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    // 데이터프레임 확인 (show)
    salesDf.show(2);

    ////////////////////////////// 3번 문제 //////////////////////////////

    // 메모리 테이블 생성
    salesDf.createOrReplaceTempView("salesTable")

    // Spark SQL로 자료 select
    var salesDf2 = spark.sql("select " +
      "REGIONID, " +
      "PRODUCT, " +
      "YEARWEEK, " +
      "cast(QTY as double), " +
      "cast(QTY * 1.2 as double) as QTY_NEW " +
      "from salesTable ")

    // SQL select 결과 확인 (show)
    salesDf2.show(2);

    ////////////////////////////// 5번 문제 //////////////////////////////

    var salesDfCol = salesDf2.columns
    var regionidNo = salesDfCol.indexOf("REGIONID")
    var productNo = salesDfCol.indexOf("PRODUCT")
    var yearweekNo = salesDfCol.indexOf("YEARWEEK")
    var qtyNo = salesDfCol.indexOf("QTY")
    var qtynewNo = salesDfCol.indexOf("QTY_NEW")

    var salesRdd = salesDf2.rdd

    var productArray = Array("PRODUCT1","PRODUCT2")
    var productSet = productArray.toSet

    var filteredRdd = salesRdd.filter(x => {
      var checkValid = false
      var yearValue = (x.getString(yearweekNo).substring(0,4)).toInt
      var weekValue = (x.getString(yearweekNo).substring(4)).toInt
      var productInfo = x.getString(productNo)

      // condition: 2016년도 이상, 53주차 제외, (Product 1, Product 2) 포함
      if ( (yearValue >= 2016 && weekValue != 53) &&
        productSet.contains(productInfo) ) {
        checkValid = true
      }
      checkValid
    })

    filteredRdd.first

    ////////////////////////////// 6번 문제 (Oracle) //////////////////////////////

    // RDD => Dataframe 으로 변환하기 (org.apache.spark.sql.types - import 필요)
    // Dataframe으로 변환해야 DB에 올리거나 파일로 저장할 수 있음
    val finalResultDf = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("QTY", DoubleType),
          StructField("NEW_QTY", DoubleType))))

    // 데이터베이스에 저장하기 (Oracle)
    var outputUrl = "jdbc:oracle:thin:@192.168.110.14:1522/XE"
    var outputDriver = "oracle.jdbc.OracleDriver"
    var outputUser = "ejkim"
    var outputPw = "ejkim22"

    var prop = new java.util.Properties
    prop.setProperty("driver", outputDriver)
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    var table = "KOPO_ST_MIDDLE_EJKIM"

    finalResultDf.write.mode("overwrite").jdbc(outputUrl, table, prop)


    ////////////////////////////// 6-2번 문제 (Postgre) //////////////////////////////
    val finalResultDf2 = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
          StructField("regionid", StringType),
          StructField("product", StringType),
          StructField("yearweek", StringType),
          StructField("qty", DoubleType),
          StructField("new_qty", DoubleType))))

    // 데이터베이스에 저장하기 (Oracle)
    var outputUrl2 = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var outputDriver2 = "org.postgresql.Driver"
    var outputUser2 = "kopo"
    var outputPw2 = "kopo"

    var prop2 = new java.util.Properties
    prop2.setProperty("driver", outputDriver2)
    prop2.setProperty("user", outputUser2)
    prop2.setProperty("password", outputPw2)
    var table2 = "kopo_st_middle_ejkim"

    finalResultDf2.write.mode("overwrite").jdbc(outputUrl2, table2, prop2)


  }

}