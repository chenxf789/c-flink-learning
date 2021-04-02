package com.cxf.data.transform

import org.apache.flink.streaming.api.scala._


object Map {
  def main(args: Array[String]): Unit = {
    //1.创建上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.定义数据源
    val stream= env.fromElements((1,"a"),(3,"c"),(2,"b"))
    //3.数据处理:(1,"a")=>(1,"1_a")
    val streamMap: DataStream[(Int, String)] = stream.map(e => {
      (e._1, e._1 + "_" + e._2)
    })
    streamMap.print()
    //执行计算
    env.execute()
  }
}
