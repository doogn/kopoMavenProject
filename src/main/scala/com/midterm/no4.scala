package com.midterm


// Java를 Scala로 변환한 코드임
// Java코드는 이클립스 참조 (Export

object WeekFunction {
  def getWeekInfo(yearWeek: String): Int = {
    val week = yearWeek.substring(4)
    val weekInfo = week.toInt
    weekInfo
  }

  def main(args: Array[String]): Unit = {
    val testValue = "201612"
    val answer = getWeekInfo(testValue)
    System.out.println("The answer is " + answer)
  }
}