package com.fortinet.flink.split

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinTesting extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  //1. 来自app的支付日志，(order-id,status,timestamp)
  val appStream = env.fromElements(
    ("order-1","success",1000L),
    ("order-2","success",2000L),
    ("order-3","success",3000L),
    ("order-4","success",4000L),
    ("order-5","success",4000L),
  ).assignAscendingTimestamps(_._3)
  //2. 来自第三方支付平台的支付信息，（order-id,status,platform-id,timestamp)
  val payStream = env.fromElements(
    ("order-1", "success","wechat", 2000L),
    ("order-2", "success","alipay", 3000L),
    ("order-3", "success","wechat", 4000L),
    ("order-4", "success","alipay", 20000L),
  ).assignAscendingTimestamps(_._4)

  appStream.join(payStream)
    .where(_._1)
    .equalTo(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))

    /**
     *  Type parameters:
     *  <IN1> – The type of the elements in the first input.
     *  <IN2> – The type of the elements in the second input.
     *  <OUT> – The type of the result elements.
     */
    .apply(new JoinFunction[(String,String,Long),(String,String,String,Long),(String,(Long,Long))] {
      override def join(first: (String, String, Long), second: (String, String, String, Long)): (String, (Long, Long)) = {
        (first._1,(first._3,second._4))
      }
    })
    .print("window join:")

  env.execute()
}
