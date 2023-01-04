package com.fortinet.flink.cep

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.util

object LoginFailDetectPro extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val loginEventStream = env.fromElements(
    LoginEvent("user_1","192.168.0.1","fail",2000L),
    LoginEvent("user_1","192.168.0.1","fail",3000L),
    LoginEvent("user_2","192.168.0.1","fail",4000L),
    LoginEvent("user_1","192.168.0.1","fail",5000L),
    LoginEvent("user_2","192.168.0.1","success",6000L),
    LoginEvent("user_2","192.168.0.1","fail",7000L),
    LoginEvent("user_2","192.168.0.1","fail",8000L),
  ).assignAscendingTimestamps(_.timestamp)

  //定义pattern检测连续两次登录失败事件
  val pattern = Pattern.begin[LoginEvent]("fail").where(_.status == "fail").times(3).consecutive()

  val patternStream:PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId),pattern)

  //定义处理规则，将检测到的匹配事件报警输出
  val resultStream = patternStream.process(new PatternProcessFunction[LoginEvent,String] {
    override def processMatch(map: util.Map[String, util.List[LoginEvent]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
      val first = map.get("fail").get(0)
      val second = map.get("fail").get(1)
      val third = map.get("fail").get(2)
      collector.collect(s"${first.userId} 连续登录三次失败！登录时间${first.timestamp}, ${second.timestamp} ${third.timestamp}")
    }
  })
  resultStream.print()
  env.execute()

}
