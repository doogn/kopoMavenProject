package com.ejkim

import org.apache.spark.sql.{Row, SparkSession}

object Example_RDD_GroupBy_2_180503 {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    var MAXVALUE = 700000

    /////////////////////////////

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

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
    var productnameNo = rawDataColumns.indexOf("product_name")

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x => {
      // boolean = true
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false
      }

      checkValid
    })
    // filteredRdd.first
    // filteredRdd = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)

    // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.


    var mapRdd = filteredRdd.map(x => {

      // 디버깅코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000 }).first
      //로직구현예정

      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if (new_qty > MAXVALUE) {
        new_qty = MAXVALUE
      }

      //출력 row 키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new )
      Row(x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty
      )
    })


    var groupRdd = mapRdd.groupBy(x=>{
        (x.getString(keyNo))
      }).map(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = data.size
      (key, size, data)
    })

    var groupRdd2 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
      }).map(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = x._2.map(x=>{x.getDouble(3)}).size
      var summation = x._2.map(x=>{x.getDouble(3)}).sum

      var average = 0.0
      if(size == 0) {
        average = 0
      } else {
          average = summation / size
        }

      var outputData = data.map(x => {
        (x.getString(0), // key 정보 (거래처, 상품)
          x.getString(1), // 주차 정보 (yearweek)
          x.getDouble(2), // Original QTY
          x.getDouble(3), // New QTY
          average) //  key(거래처, 상품)의 평균
      })

      outputData
    })

    var groupRdd7 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).map(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = x._2.map(x=>{x.getDouble(3)}).size
      var summation = x._2.map(x=>{x.getDouble(3)}).sum

      var average = 0.0
      if(size == 0) {
        average = 0
      } else {
        average = summation / size
      }
      (key,average)  // 이렇게 하면 이건 .toDF()로 바로 DataFrame화 가능
    })




    var groupRdd3 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).flatMap(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = x._2.map(x=>{x.getDouble(3)}).size
      var summation = x._2.map(x=>{x.getDouble(3)}).sum

      var average = 0.0
      if(size == 0) {
        average = 0
      } else {
        average = summation / size
      }

      var outputData = data.map(x => {
        Row(x.getString(0), // key 정보 (거래처, 상품)
          x.getString(1), // 주차 정보 (yearweek)
          x.getDouble(2), // Original QTY
          x.getDouble(3), // New QTY
          average) //  key(거래처, 상품)의 평균
      })

      outputData

    })

    var groupRdd4 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).flatMap(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = x._2.map(x=>{x.getDouble(3)}).size
      var summation = x._2.map(x=>{x.getDouble(3)}).sum

      var average = 0.0
      if(size == 0) {
        average = 0
      } else {
        average = summation / size
      }

      var ratio = 1.0d  // ratio 값 초기화

      var outputData = data.map(x => {

        //
        (x.getString(0), // key 정보 (거래처, 상품)
          x.getString(1), // 주차 정보 (yearweek)
          x.getDouble(2), // Original QTY
          x.getDouble(3), // New QTY
          average,  // 평균
          x.getDouble(3) / average) //  ratio
      })

      outputData

    })

    // flatmap을 사용했을 때 => Row를 안 쓰면 바로 .toDF()를 사용해서 DataFrame으로 변경 가능
    // flatmap을 사용했을 때 => Row를 쓰면 .toDF()로 DataFrame 못 만듦 (Row는 어떨때 쓰는가?)

    // RDD[...]  ... 안에 Iterable 형태의 자료가 있으면 .toDF()로 데이터프레임 못 만듦 => String, Double 등 명확한 형태 있어야
    // ==> 없으면 structur 부여해야 함


    var groupRdd8 = mapRdd.groupBy(x=>{
      (x.getString(keyNo))
    }).map(x=> {
      //key값 String
      var key = x._1
      //기본 데이터 156
      var data = x._2
      // Row 형태일 때는 getString(Double,...)으로 원하는 값을 가져오고 (index no.는 0부터 시작 => getString(0), not getString(1))
      // Row 형태가 아닐 때는 x._n 으로 원하는 값을 가져온다 (1부터 시작 => 첫번째 값이 x._1, not x._0)
      var size = x._2.map(x=>{x.getDouble(3)}).size
      var summation = x._2.map(x=>{x.getDouble(3)}).sum

      var average = 0.0
      if(size == 0) {
        average = 0
      } else {
        average = summation / size
      }

      var outputData = data.map(x => {
        Row(x.getString(0), // key 정보 (거래처, 상품)
          x.getString(1), // 주차 정보 (yearweek)
          x.getDouble(2), // Original QTY
          x.getDouble(3), // New QTY
          average) //  key(거래처, 상품)의 평균
      })

      outputData
    })

  }



}