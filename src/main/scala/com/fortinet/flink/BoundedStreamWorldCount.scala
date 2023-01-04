package com.fortinet.flink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BoundedStreamWorldCount extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val stream = env.readTextFile("input/hello.txt")

  stream.flatMap(_.split(" "))
    .map((_,1))
    .keyBy(data=>data._1)  //.keyBy(_._1) instead of groupBy
    .sum(1).print()

  env.execute()
}
