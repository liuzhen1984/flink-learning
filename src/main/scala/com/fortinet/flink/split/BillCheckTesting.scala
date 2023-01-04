package com.fortinet.flink.split

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object BillCheckTesting extends App {
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

  appStream.connect(payStream)
    .keyBy(_._1,_._1)

    /**
     *  Type parameters:
     *  <IN1> – Type of the first input.
     *  <IN2> – Type of the second input.
     *  <OUT> – Output type.
     */
    .process(new CoProcessFunction[(String,String,Long),(String,String,String,Long),(String,String)] {
      //定义状态变量，用来保存已经到达的时间
      var appEvent:ValueState[(String,String,Long)] = _
      var thirdpartyEvent:ValueState[(String,String,String,Long)] = _


      //初始化定义state
      override def open(parameters: Configuration): Unit = {
        appEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, Long)]("app-event",classOf[(String,String,Long)]))
        thirdpartyEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, String, Long)]("pay-event",classOf[(String,String,String,Long)]))
      }

      override def processElement1(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
         if(thirdpartyEvent.value()!=null){
           out.collect((value._1,"completed"))
           //清空状态
           thirdpartyEvent.clear()
         }else{
           ctx.timerService().registerEventTimeTimer(value._3+1000)
           appEvent.update(value)
         }
      }

      override def processElement2(value: (String, String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
        if (appEvent.value() != null) {
          out.collect((value._1, "completed"))
          //清空状态
          appEvent.clear()
        } else {
          ctx.timerService().registerEventTimeTimer(value._4 + 1000)
          thirdpartyEvent.update(value)
        }
      }

      override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
        //判断状态是否为空，如果不为空，表示另外一个有问题
        if(appEvent.value()!=null){
          out.collect((appEvent.value()._1,"third failed"))
        }
        if(thirdpartyEvent.value()!=null){
          out.collect((thirdpartyEvent.value()._1,"app failed"))
        }
        appEvent.clear()
        thirdpartyEvent.clear()
      }
    })
    .print()

  env.execute()
}
