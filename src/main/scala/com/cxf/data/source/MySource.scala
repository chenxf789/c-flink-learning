package com.cxf.data.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

//自定义source
object MySource {
  def main(args: Array[String]): Unit = {
    //(1)创建上下文运行环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //(2)定义数据源：自定义source
    val stream: DataStream[SensorReading] = env.addSource(new MySensorSource())
    //(3)数据处理
    stream.print().setParallelism(1)
    //(4)执行程序
    env.execute()
  }

  class MySensorSource extends SourceFunction[SensorReading]{
    //表示数据源是否正在运行
    var running:Boolean=true
    override def cancel(): Unit = {
      running=false
    }

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      //初始化一个随机发生器
      val rand = new Random()
      val curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i=>("sensor_"+i,65+rand.nextGaussian()*20))
      while (running){
        //更新温度值
        curTemp.map(t=>(t._1,t._2+rand.nextGaussian()))
        //获取当前时间戳
        val curTime: Long = System.currentTimeMillis()
        curTemp.foreach(t=>ctx.collect(SensorReading(t._1,curTime,t._2)))
        Thread.sleep(100)
      }
    }
  }
}
