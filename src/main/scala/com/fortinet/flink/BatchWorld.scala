package com.fortinet.flink

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object BatchWorld extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val lineDataSet = env.readTextFile("input/hello.txt")
  val group = lineDataSet
    .flatMap(_.split(" "))
    .map((_,1))   // (hello,1),(do,1),(hello,1)
    .groupBy(0); // (helle,((helle,1),(hello,1)), do,(do,1))
  group
    .sum(1).print()
}
