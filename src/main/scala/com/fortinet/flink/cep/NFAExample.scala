package com.fortinet.flink.cep

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object NFAExample extends App {

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

  val resultStream = loginEventStream.keyBy(_.userId)
    .flatMap(new StateMatchingMapper())

  resultStream.print()
  env.execute()

  class StateMatchingMapper extends RichFlatMapFunction[LoginEvent,String] {
    //定义一个状态机状态
    lazy val state = getRuntimeContext.getState(new ValueStateDescriptor[State]("state",classOf[State]))
    override def flatMap(value: LoginEvent, out: Collector[String]): Unit = {
      if(state.value()==null){
        state.update(Initial)
      }
      val nextState = transition(state.value(),value.status)

      nextState match {
        case Matched=> out.collect(s"${value.userId} 连续三次失败")
        case Terminal=> state.update(Initial)
        case _=> state.update(nextState)
      }
    }

    sealed trait State
    case object Initial extends State
    case object Terminal extends State
    case object Matched extends State
    case object S1 extends State
    case object S2 extends State
    //定义状态转移函数
    def transition(state:State,eventType:String):State={
      (state,eventType) match {
        case (Initial,"success")=>Terminal
        case (Initial,"fail")=>S1
        case (S1,"success")=>Terminal
        case (S1,"fail")=>S2
        case (S2,"success")=>Terminal
        case (S2,"fail") => Matched
        case _ => Initial
      }
    }
  }
}
