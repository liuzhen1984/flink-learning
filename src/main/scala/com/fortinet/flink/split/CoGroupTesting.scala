package com.fortinet.flink.split

import com.fortinet.flink.Event
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.lang

object CoGroupTesting extends App {
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
  orderStream.coGroup(pvStream)
    .where(_._2)
    .equalTo(_.user)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))

    /**
     *  Type parameters:
     *  <IN1> – The data type of the first input data stream.
     *  <IN2> – The data type of the second input data stream.
     *  <O> – The data type of the returned elements.
     */
    .apply(new CoGroupFunction[(String,String,Long),Event,String] {
      override def coGroup(first: lang.Iterable[(String, String, Long)], second: lang.Iterable[Event], out: Collector[String]): Unit = {
        out.collect(first+"=>"+second)
      }
    })
    .print()
  env.execute()
}
