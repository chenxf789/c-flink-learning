package com.cxf.data.transform

import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object FlatMap {
  def main(args: Array[String]): Unit = {
    //1.创建上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.定义数据源
    val stream: DataStream[String] = env.fromCollection(List("aaa bbb ccc","aaa ccc"))
    //3.数据处理:List("aaa bbb ccc","aaa ccc")=>{aaa,bbb,ccc,aaa,ccc}
    val streamFlatMap: DataStream[String] = stream.flatMap(e => {
      val strings: mutable.ArrayOps[String] = e.split(" ")
      strings
    })
    streamFlatMap.print()
    //执行计算
    env.execute()
  }
}
