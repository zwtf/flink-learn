package com.flink.learn.wc

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

object WcApp {

  def main(args: Array[String]): Unit = {
    //构造执行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val input = "D:\\6_tmp\\wc.txt"

    val ds = environment.readTextFile(input)

    //flatMap 和map需要隐式转换
    //以flatMap为例,这个方法是一个柯里化写法,需要一个隐式参数:
    // (implicit evidence$16 : org.apache.flink.api.common.typeinfo.TypeInformation[R], evidence$17 : scala.reflect.ClassTag[R])
    // 后面的evidence$17已经自动推断成了String.clazz,TypeInformation[String],作用域里面没有找到
    //通过createTypeInformation 生成一个TypeInformation对象
    //不加隐式转换，有以下两种写法:
    //写法一:
    //val information:TypeInformation[String] = org.apache.flink.api.scala.createTypeInformation
    //val aggDs = ds.flatMap(_.split(" "))(information,ClassTag[Nothing])
    //写法二: 定义一个隐式变量，会自动匹配到
    //implicit val information: TypeInformation[String] = org.apache.flink.api.scala.createTypeInformation
    //val aggDs = ds.flatMap(_.split(" "))

    import org.apache.flink.api.scala.createTypeInformation
    val aggDs = ds
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .sortPartition(1, Order.DESCENDING) //按个数降序排列

    aggDs.print()

  }

}
