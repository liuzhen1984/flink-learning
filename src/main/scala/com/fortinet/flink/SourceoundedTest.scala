package com.fortinet.flink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

case class Event(user:String,url:String,timestamp:Long)

object SourceoundedTest {
  def main(args:Array[String]):Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //default value is the local env cpu number
    val stream :DataStream[Int]= env.fromElements(1,2,3,4);

    val stream2 :DataStream[Event]= env.fromElements(Event("Mary","/home",1000L))

    val clicks = List(Event("Jack","home",1000L))
    val stream3 = env.fromCollection(clicks)

    stream.print("stream1")
    stream2.print("stream2")
    stream3.print("stream3")

    env.execute()
  }

}
