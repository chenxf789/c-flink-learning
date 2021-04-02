package com.cxf.data.source

import org.apache.flink.streaming.api.scala._

//从元祖读取数据
object Elements {
  def main(args: Array[String]): Unit = {
    //1.定义上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.定义数据源
    val stream: DataStream[(Int, String)] = env.fromElements((1,"a"),(3,"c"),(2,"b"))
    //3.数据处理
    stream.print()
    //4.执行程序
    env.execute()
  }
}
