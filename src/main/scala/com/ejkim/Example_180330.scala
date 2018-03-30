package com.ejkim

object Example_180330 {

  def main(args: Array[String]): Unit = {

    // 예제 1. yearValue, week Value 함수화

    def quizDef(inputValue: String): (Int, Int) = {
      var target = inputValue.split(";")
      var yearValue = target(0)
      var weekValue = target(1)
      (yearValue.toInt, weekValue.toInt)
    }

    var test = "2017;34"

    var answer = quizDef(test)

    println(answer)
  }

}
