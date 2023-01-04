package com.fortinet.flink

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWorldCount extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment


  val parameterTool = ParameterTool.fromArgs(args)
//  parameterTool.get("hostname")
//  parameterTool.getInt("port")

  val stream = env.socketTextStream("127.0.0.1",19999)


  stream.flatMap(_.split(" "))
    .map((_,1))
    .keyBy(data=>data._1)  //.keyBy(_._1) instead of groupBy
    .sum(1).print()

  stream.filter(i=>i == 0)
  stream.filter(_==0)
  env.execute()
}

class MyKeySelector extends KeySelector[Event,String] {
  override def getKey(value: Event): String = value.user
}


