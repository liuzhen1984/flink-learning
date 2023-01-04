package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CustomTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new ClickSource())
    stream.partitionCustom(new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        key match {
          case "Mary" => 0
          case "Lucy" => 1
          case "Jack" =>2
          case _ => 3
        }
      }
    },event=>event.user).print("custom").setParallelism(4)
    env.execute()
  }

}

