package com.fortinet.flink.cep

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util

case class OrderEvent(userId:String,orderId:String,eventType:String,timestamp:Long)
object OrderDetect extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val orderEventStream = env.fromElements(
    OrderEvent("user_1","order_1","create",1000L),
    OrderEvent("user_2","order_2","create",2000L),
    OrderEvent("user_1","order_1","modify",10*1000L),
    OrderEvent("user_1","order_1","pay",60*1000L),
    OrderEvent("user_2","order_3","create",10*60*1000L),
    OrderEvent("user_2","order_3","pay",20*60*1000L),
  ).assignAscendingTimestamps(_.timestamp).keyBy(_.orderId)

  //定义pattern检测连续两次登录失败事件
  val pattern = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
    .followedBy("pay").where(_.eventType=="pay")
    .within(Time.minutes(15))


  val patternStream:PatternStream[OrderEvent] = CEP.pattern(orderEventStream,pattern)

  //定义处理规则，将检测到的匹配事件报警输出
  val payedOrderStream = patternStream.process(new OrderProcess())

  payedOrderStream.print("payed")
  payedOrderStream.getSideOutput(new OutputTag[String]("timeout")).print("timeout")
  env.execute()


  class OrderProcess extends PatternProcessFunction[OrderEvent,String] with TimedOutPartialMatchHandler[OrderEvent] {
    override def processMatch(map: util.Map[String, util.List[OrderEvent]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
      val payEvent = map.get("pay").get(0)
      collector.collect(s"订单${payEvent.orderId} 已成功支付")
    }

    override def processTimedOutMatch(map: util.Map[String, util.List[OrderEvent]], context: PatternProcessFunction.Context): Unit = {
      val event = map.get("create").get(0)
      context.output(new OutputTag[String]("timeout"),s"订单${event.orderId} 超时支付！，用户${event.userId}")
    }
  }
}
