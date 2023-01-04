package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BoardcastTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new ClickSource())
    stream.global.print("global").setParallelism(4)
    env.execute()
  }

}

