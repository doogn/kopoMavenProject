package com.ejkim


import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import edu.princeton.cs.introcs.StdStats  // mean, stddev 함수가 포함된 라이브러리 import

object Quiz_180503 {

/////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////  Function Definition ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////
// Function: Return movingAverage result
// Input:
//   1. Array : targetData: inputsource
//   2. Int   : myorder: section
// output:
//   1. Array : result of moving average
def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
  val length = targetData.size
  if (myorder > length || myorder <= 2) {
  throw new IllegalArgumentException
} else {
  var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

  if (myorder % 2 == 0) {
  maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
}
  maResult.toArray
}
}




  ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
  var spark = SparkSession.builder().config("spark.master","local").getOrCreate()



  /////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // Path setting
  //    var dataPath = "./data/"
  //    var mainFile = "kopo_channel_seasonality_ex.csv"
  //    var subFile = "kopo_product_master.csv"
  //
  //    var path = "c:/spark/bin/data/"
  //
  //    // Absolute Path
  //    //kopo_channel_seasonality_input
  //    var mainData = spark.read.format("csv").option("header", "true").load(path + mainFile)
  //    var subData = spark.read.format("csv").option("header", "true").load(path + subFile)
  //
  //    spark.catalog.dropTempView("maindata")
  //    spark.catalog.dropTempView("subdata")
  //    mainData.createTempView("maindata")
  //    subData.createOrReplaceTempView("subdata")
  //
  //    /////////////////////////////////////////////////////////////////////////////////////////////////////
  //    ////////////////////////////////////  Data Refining using sql////////////////////////////////////////
  //    /////////////////////////////////////////////////////////////////////////////////////////////////////
  //    var joinData = spark.sql("select a.regionid as accountid," +
  //      "a.product as product, a.yearweek, a.qty, b.productname " +
  //      "from maindata a left outer join subdata b " +
  //      "on a.productgroup = b.productid")
  //
  //    joinData.createOrReplaceTempView("keydata")
  // 1. data loading
  //////////////////////////////////////////////////////////////////////////////////////////////////
  var staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"
  staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"

  val selloutDataFromOracle = spark.read.format("jdbc").
  options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

  selloutDataFromOracle.createOrReplaceTempView("keydata")

  println(selloutDataFromOracle.show())
  println("oracle ok")

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // 2. data refining
  //////////////////////////////////////////////////////////////////////////////////////////////////

  //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
  //      "salesid, salesname, productgroup, product, item," +
  //      "yearweek, year, week, " +
  //      "cast(qty as double) as qty," +
  //      "cast(target as double) as target," +
  //      "idx from selloutTable where 1=1"
  var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
  "a.regionid as accountid, " +
  "a.product, " +
  "a.yearweek, " +
  "cast(a.qty as String) as qty, " +
  "'test' as productname from keydata a" )

  rawData.show(2)

  var rawDataColumns = rawData.columns
  var keyNo = rawDataColumns.indexOf("keycol")
  var accountidNo = rawDataColumns.indexOf("accountid")
  var productNo = rawDataColumns.indexOf("product")
  var yearweekNo = rawDataColumns.indexOf("yearweek")
  var qtyNo = rawDataColumns.indexOf("qty")
  var productnameNo = rawDataColumns.indexOf("productname")

  var rawRdd = rawData.rdd

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // The abnormal value is refined using the normal information
  var filterRdd = rawRdd.filter(x=>{

  // Data comes in line by line
  var checkValid = true
  // Assign yearweek information to variables
  var week = x.getString(yearweekNo).substring(4,6).toInt
  // Assign abnormal to variables
  var standardWeek = 52

  // filtering
  if (week > standardWeek)
{
  checkValid = false
}
  checkValid
})

  // key, account, product, yearweek, qty, productname
  var mapRdd = filterRdd.map(x=>{
  var qty = x.getString(qtyNo).toDouble
  var maxValue = 700000
  if(qty > 700000){qty = 700000}
  Row( x.getString(keyNo),
  x.getString(accountidNo),
  x.getString(productNo),
  x.getString(yearweekNo),
  qty, //x.getString(qtyNo),
  x.getString(productnameNo))
})

  //////////// 1. map으로 구하기

  var groupRdd1 = mapRdd.groupBy(x=>{
    (x.getString(keyNo))
  }).map(x=> {

    var key = x._1
    var data = x._2

    var size = x._2.map(x=>{x.getDouble(4)}).size
    var summation = x._2.map(x=>{x.getDouble(4)}).sum

    var average = 0.0
    if(size == 0) {
      average = 0
    } else {
      average = summation / size
    }

    var devsum = x._2.map(x=>{Math.pow((x.getDouble(4)-average),2)}).sum
    var stddev = 0.0
    if(size == 0) {
      stddev = 0
    } else {
      stddev = Math.pow((devsum / size), 0.5)
    }

    var outputData = data.map(x => {
      (x.getString(0),
        size,
        average,
        stddev)
    })

    outputData

  })


  //////////// 2. flatMap으로 구하기

  var groupRdd2 = mapRdd.groupBy(x=>{
    (x.getString(keyNo))
  }).flatMap(x=> {

    var key = x._1
    var data = x._2

    var size = x._2.map(x=>{x.getDouble(4)}).size
    var summation = x._2.map(x=>{x.getDouble(4)}).sum

    var average = 0.0
    if(size == 0) {
      average = 0
    } else {
      average = summation / size
    }

    var devsum = x._2.map(x=>{Math.pow((x.getDouble(4)-average),2)}).sum
    var stddev = 0.0
    if(size == 0) {
      stddev = 0
    } else {
      stddev = Math.pow((devsum / size), 0.5)
    }

    var outputData = data.map(x => {
      Row(x.getString(0),
        size,
        average,
        stddev)
    })

    outputData

  })

  //////////// 3. stdlib.jar 파일 이용해서 구하기 (map)

  var groupRdd3 = mapRdd.groupBy(x=>{
    (x.getString(keyNo))
  }).map(x=> {

    var key = x._1
    var data = x._2
    var data2 = x._2.map(x=>{x.getDouble(4)}).toArray  // mean, stddev 함수 활용을 위해 Array로 변환

    var size = data2.size
    var average = StdStats.mean(data2)  // Stdstats 라이브러리의 mean 함수 사용
    var stddeviation = StdStats.stddev(data2)  // Stdstats 라이브러리의 stddev 함수 사용

    var outputData = data.map(x => {
      (x.getString(0),
        size,
        average,
        stddeviation)
    })

    outputData

  })

  //////////// 4. map으로 구하기 (Ratio 포함)

  var groupRdd4 = mapRdd.groupBy(x=>{
    (x.getString(keyNo))
  }).map(x=> {

    var key = x._1
    var data = x._2

    var size = x._2.map(x=>{x.getDouble(4)}).size
    var summation = x._2.map(x=>{x.getDouble(4)}).sum

    var average = 0.0
    if(size == 0) {
      average = 0
    } else {
      average = summation / size
    }

    var devsum = x._2.map(x=>{Math.pow((x.getDouble(4)-average),2)}).sum
    var stddev = 0.0
    if(size == 0) {
      stddev = 0
    } else {
      stddev = Math.pow((devsum / size), 0.5)
    }

    var ratio = 1.0  // ratio 구하기

    var outputData = data.map(x => {

      ratio = x.getDouble(qtyNo) / average  // average는 같지만 qty가 각기 달라 의미있는 ratio
      (x.getString(0),
        size,
        average,
        stddev,
        ratio)
    })

    outputData

  })

  // A01, PRODUCT1, 201401, 3000, 0.7
  // A01, PRODUCT1, 201402, 3000, 0.7

  // 주차 정보로 GROUPBY => 1~52주차 각 주를 기준으로 평균값 정리 =>


}
