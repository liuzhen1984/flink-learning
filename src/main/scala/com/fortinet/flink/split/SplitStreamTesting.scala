package com.fortinet.flink.split

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SplitStreamTesting extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val stream = env.addSource(new ClickSource())

  val maryStream = stream.filter(_.user=="Mary")
  val bobStream = stream.filter(_.user.toLowerCase=="bob")
  val elseStream = stream.filter(data=> data.user.toLowerCase!="bob" && data.user.toLowerCase()!="mary")

  maryStream.print("mary")
  bobStream.print("bob")
  elseStream.print("else")
  env.execute()

}
