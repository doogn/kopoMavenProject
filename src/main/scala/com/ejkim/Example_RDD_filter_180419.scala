package com.ejkim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Example_RDD_filter_180419 {


  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    //RDD 정제 연산 (연주차 정보가 52보다 큰값은 제거)

    var filteredRdd = rawRdd.filter(x => {
      // boolean = true로 설정
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 Int타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      // 비교한 후 주차정보가 53 이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false
      }
      checkValid
    })

    //RDD 정제 연산 (상품정보가 PRODUCT1,2 인 정보만 필터링(선택))

    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT1","PRODUCT2")

    // Set 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = rawRdd.filter(x => {
      // boolean = true로 설정
      var checkValid = false
      // 찾기: product 인덱스로 상품정보만 Set타입으로 변환
      // 데이터 특정 행의 product 컬럼 인덱스를 활용하여 데이터 대입
      //
      var productInfo = x.getString(productNo)
      // 비교한 후 상품정보가 PRODUCT 1,2인 경우 레코드 선택
      if (productSet.contains(productInfo)) {
        checkValid = true
        // 각 값을 하나씩 따로따로 비교하면 비효율적이 되므로 이렇게 통으로 비교할 것
      }
      checkValid
    })

    // RDD 형태 데이터의 첫 행 보여주기
    resultRdd.first
    // RDD 형태 데이터의 특정행(위부터 n행) 보여주기
    resultRdd.take(3).foreach(println)
    // RDD 형태의 데이터 전체를 보여주기
    resultRdd.collect().foreach(println)

    // RDD => Dataframe 으로 변환하기 (org.apache.spark.sql.types - import 필요)
    // Dataframe으로 변환해야 DB에 올리거나 파일로 저장할 수 있음
    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEY", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("VOLUME", DoubleType),
          StructField("PRODUCT_NAME", StringType))))

  }
}







