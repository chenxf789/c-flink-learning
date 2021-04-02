package com.cxf.data.source
import org.apache.flink.streaming.api.scala._

//从集合读取数据
//1、定义样例类，传感器id、时间戳、温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)
//2、实现代码
object Collection {
  def main(args: Array[String]): Unit = {
    //(1)创建上下文运行环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(2)定义数据源：从集合读取数据
    val stream: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    //(3)数据处理
    stream.print().setParallelism(1)
    //(4)执行程序
    env.execute()
  }
}
