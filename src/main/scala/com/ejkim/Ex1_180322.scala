package com.ejkim

import org.apache.spark.sql.SparkSession

object Ex1_180322 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var a = 10;
    println(10);

    var testArray = Array(22, 33, 44, 55, 60, 70)

    var answer = testArray.filter(x=>{
      var data = x.toString
      var dataSize = data.length
      var lastChar = data.substring(dataSize - 1).toString
      lastChar.equalsIgnoreCase("0") })


  }

}
