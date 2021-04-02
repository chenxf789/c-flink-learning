package com.cxf.data.source

import org.apache.flink.streaming.api.scala._

//从文件读取数据
object File {
  def main(args: Array[String]): Unit = {
    //(1)创建上下文运行环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //(2)定义数据源：从文件读取数据
    val path: String = classOf[Nothing].getClassLoader.getResource("file.txt").getPath
    val stream: DataStream[String] = env.readTextFile(path)
    //(3)数据处理
    stream.print()
    //(4)执行程序
    env.execute()
  }
}
