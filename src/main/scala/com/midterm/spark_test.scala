package com.midterm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object spark_test {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var salesDb = "KOPO_CHANNEL_SEASONALITY_NEW"

    val salesDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> salesDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    // 1번답: 데이터 불러오고 확인
    salesDf.show(2);

    salesDf.createOrReplaceTempView("salesTable")

    // 2번답: SQL분석
    var salesDf2 = spark.sql("select " +
      "REGIONID, " +
      "PRODUCT, " +
      "YEARWEEK, " +
      "cast(QTY as double), " +
      "cast(QTY * 1.2 as double) as QTY_NEW " +
      "from salesTable")

    // (3번답: 함수 => 별도 캡쳐하여 메일 첨부)

    // 4번답: 정제
    var salesDfCol = salesDf2.columns
    var regionidNo = salesDfCol.indexOf("REGIONID")
    var productNo = salesDfCol.indexOf("PRODUCT")
    var yearweekNo = salesDfCol.indexOf("YEARWEEK")
    var qtyNo = salesDfCol.indexOf("QTY")
    var qtynewNo = salesDfCol.indexOf("QTY_NEW")

    var salesRdd = salesDf2.rdd

    var baseYear = 2016
    var baseWeek = 52
    var productArray = Array("PRODUCT1", "PRODUCT2")
    var productSet = productArray.toSet

    var filteredRdd = salesRdd.filter(x => {
      var checkValid = false
      var yearValue = (x.getString(yearweekNo).substring(0, 4)).toInt
      var weekValue = (x.getString(yearweekNo).substring(4)).toInt
      var productInfo = x.getString(productNo)

      // condition: 2016년도 이상, 52주차 제외, (Product 1, Product 2) 포함
      if ((yearValue >= baseYear && weekValue != baseWeek) &&
        productSet.contains(productInfo)) {
        checkValid = true
      }
      checkValid
    })

    // 5번답: 저장
    var lowerCol = salesDf2.columns.map(x=>{x.toLowerCase()})

    val finalResultDf = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
          StructField(lowerCol(0), StringType),
          StructField(lowerCol(1), StringType),
          StructField(lowerCol(2), StringType),
          StructField(lowerCol(3), DoubleType),
          StructField(lowerCol(4), DoubleType))))

    var outputUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var outputDriver = "org.postgresql.Driver"
    var outputUser = "kopo"
    var outputPw = "kopo"

    var prop = new java.util.Properties
    prop.setProperty("driver", outputDriver)
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    var table = "KOPO_ST_MIDDLE_EJKIM"

    finalResultDf.write.mode("overwrite").jdbc(outputUrl, table, prop)

  }

}
