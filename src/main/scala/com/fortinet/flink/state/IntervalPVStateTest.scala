package com.fortinet.flink.state

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.common.time._

object IntervalPVStateTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)
    .keyBy(_.user)
    .process(new IntervalPV)
    .print()

  env.execute()

  /**
   * Type parameters:
   * <K> – Type of the key.
   * <I> – Type of the input elements.
   * <O> – Type of the output elements.
   */
  class IntervalPV extends KeyedProcessFunction[String, Event, String] {

    val stateConfig = StateTtlConfig.newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) //读或写就会更新时间
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //过期后不再返回
      .build()
    val valueStateDescriptor = new ValueStateDescriptor[Long]("count", classOf[Long])
    valueStateDescriptor.enableTimeToLive(stateConfig)
    //可以替换open中初始化

    lazy val countState: ValueState[Long] = getRuntimeContext.getState(valueStateDescriptor)


    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))

    override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
      val count = countState.value()
      countState.update(count + 1)
      //每个十秒输出一次
      if (timeState.value() <= 0L) {
        ctx.timerService().registerEventTimeTimer(value.timestamp + 10*1000L)
        timeState.update(value.timestamp + 10*1000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(s"用户:${ctx.getCurrentKey} 的pv值为： ${countState.value()} timer= ${timeState.value()}")
        //清理定时器
        timeState.clear()
    }
  }
}
