package com.fortinet.flink.split

import com.fortinet.flink.Event
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinTesting extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  //1. 来自app的支付日志，(order-id,status,timestamp)
  val orderStream = env.fromElements(
    ("order-1","Bob",1000L),
    ("order-2","Alice",2000L),
    ("order-3","Bob",3000L),
    ("order-4","Alice",4000L),
    ("order-5","Cary",4000L),
  ).assignAscendingTimestamps(_._3)
  //2. 来自第三方支付平台的支付信息，（order-id,status,platform-id,timestamp)
  val pvStream = env.fromElements(
    Event("Bob","./cart",2000L),
    Event("Alice","./prod?id=100",3000L),
    Event("Alice","./prod?id=200",3500L),
    Event("Bob","./prod?id=2",2500L),
    Event("Alice","./prod?id=300",36000L),
    Event("Bob","./home",30000L),
    Event("Bob","./prod?id=1",23000L),
    Event("Bob","./prod?id=3",33000L),
  ).assignAscendingTimestamps(_.timestamp)

  //订单时间前后一段时间范围内的浏览数据
  orderStream.keyBy(_._2)
    .intervalJoin(pvStream.keyBy(_.user))
    .between(Time.seconds(-5),Time.seconds(10))
    /**
    Type parameters:
    <IN1> – Type of the first input
    <IN2> – Type of the second input
    <OUT> – Type of the output
     */
    .process(new ProcessJoinFunction[(String,String,Long),Event,String] {
      override def processElement(left: (String, String, Long), right: Event, ctx: ProcessJoinFunction[(String, String, Long), Event, String]#Context, out: Collector[String]): Unit = {
        out.collect(left + " " + right)
      }
    })
    .print("interval join:")

  env.execute()
}
