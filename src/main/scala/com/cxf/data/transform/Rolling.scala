package com.cxf.data.transform

import org.apache.flink.streaming.api.scala._

object Rolling {
  def main(args: Array[String]): Unit = {
    //1.创建上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.定义数据源
    val stream= env.fromElements((1,99),(1,100),(3,50),(3,45),(3,120))
    //3.数据处理:sum()、min()、max()、minBy()、maxBy()
    print("====================sum()")
    val streamKeyBySum: DataStream[(Int, Int)] = stream.keyBy(e=>e._1).sum(1)
    streamKeyBySum.print().setParallelism(1)
    print("====================min()")
    val streamKeyByMin: DataStream[(Int, Int)] = stream.keyBy(e=>e._1).min(1)
    streamKeyByMin.print().setParallelism(1)
    print("====================max()")
    val streamKeyByMax: DataStream[(Int, Int)] = stream.keyBy(e=>e._1).max(1)
    streamKeyByMax.print().setParallelism(1)
    print("====================minBy()")
    val streamKeyByMinBy: DataStream[(Int, Int)] = stream.keyBy(e=>e._1).minBy(1)
    streamKeyByMinBy.print().setParallelism(1)
    print("====================maxBy()")
    val streamKeyByMaxBy: DataStream[(Int, Int)] = stream.keyBy(e=>e._1).maxBy(1)
    streamKeyByMaxBy.print().setParallelism(1)
    //执行计算
    env.execute()
  }
}
