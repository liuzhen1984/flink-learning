package com.fortinet.flink.state

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.state._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

case class Action(userId:String,action:String)
case class Pattern(action1:String,action2:String)
object BroadcastStateTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val actionStream = env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)
    .filter(event=>{
      event.url.contains("login")||event.url.contains("pay")||event.url.contains("buy")
    }).map(data=>Action(data.user,data.url.split("./")(1)))

  val patternStream = env.fromElements(
    Pattern("login","pay"),
//    Pattern("login","buy")
  )

  //定义广播状态的描述器
  val patterns = new MapStateDescriptor[Unit,Pattern]("patterns",classOf[Unit],classOf[Pattern])
  val broadcastStream = patternStream.broadcast(patterns)
  //链接两条流，进行处理
  actionStream.keyBy(_.userId)
    .connect(broadcastStream)
    .process(new PatternEvaluation)
    .print()


  env.execute()

  /**
   * Type parameters:
   *  <KS> – The key type of the input keyed stream.
   *  <IN1> – The input type of the keyed (non-broadcast) side.
   *  <IN2> – The input type of the broadcast side.
   *  <OUT> – The output type of the operator.
   */
  class PatternEvaluation extends KeyedBroadcastProcessFunction[String,Action,Pattern,(String,Pattern)] {
    //定义一个值状态保存上一个状态
    lazy val prevActionState = getRuntimeContext.getState(new ValueStateDescriptor[Action]("prev-action",classOf[Action]))
    override def processElement(value: Action, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#ReadOnlyContext, out: Collector[(String, Pattern)]): Unit = {
      //从广播状态中获取pattern
      val pattern = ctx.getBroadcastState(new MapStateDescriptor[Unit,Pattern]("patterns",classOf[Unit],classOf[Pattern])).get(Unit)
      if(prevActionState.value()!=null && pattern!=null){

        if(prevActionState.value().action==pattern.action1 && value.action==pattern.action2){
          out.collect((ctx.getCurrentKey,pattern))
        }
      }
      prevActionState.update(value)
    }

    override def processBroadcastElement(value: Pattern, ctx: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#Context, out: Collector[(String, Pattern)]): Unit = {
      val bcState = ctx.getBroadcastState(new MapStateDescriptor[Unit,Pattern]("patterns",classOf[Unit],classOf[Pattern]))
      bcState.put(Unit,value)

    }
  }
}
