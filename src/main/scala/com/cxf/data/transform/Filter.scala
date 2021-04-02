package com.cxf.data.transform

import org.apache.flink.streaming.api.scala._

object Filter {
  def main(args: Array[String]): Unit = {
    //1.创建上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.定义数据源
    val stream= env.fromElements((1,99),(1,100),(3,50),(3,45),(3,120))
    //3.数据处理:
    val streamFilter: DataStream[(Int, Int)] = stream.filter(e=>{e._1>=3})
    streamFilter.print()
    //执行计算
    env.execute()
  }
}
