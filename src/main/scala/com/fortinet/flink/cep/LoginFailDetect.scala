package com.fortinet.flink.cep

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util

case class LoginEvent(userId:String,ip:String,status:String,timestamp:Long)
object LoginFailDetect extends App {

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
  val pattern = Pattern.begin[LoginEvent]("firstFail").where(_.status == "fail")
    .next("secondFail").where(_.status=="fail")
    .next("thirdFail").where(_.status=="fail")

  val patternStream:PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId),pattern)

  //定义处理规则，将检测到的匹配事件报警输出
  patternStream.select(new PatternSelectFunction[LoginEvent,String] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
      val firstFail = map.get("firstFail").get(0)
      val secondFail = map.get("secondFail").get(0)
      val thirdFail = map.get("thirdFail").get(0)
      s"${firstFail.userId} 连续登录三次失败！登录时间${firstFail.timestamp}, ${secondFail.timestamp} ${thirdFail.timestamp}"
    }
  }).print()
  env.execute()

}
