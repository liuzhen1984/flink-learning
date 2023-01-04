package com.fortinet.flink.state

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.state.{ListState, _}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable

object BufferingSinkTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)
    .addSink(new BufferingSink(10))

  env.execute()

  class BufferingSink(threshold:Int) extends SinkFunction[Event] with CheckpointedFunction{
    //定义列表状态保存要缓存的数据
    var eventState : ListState[Event] = _
    val bufferedList = mutable.ListBuffer[Event]()

    override def invoke(value: Event, context: SinkFunction.Context): Unit = {
      bufferedList += value
      if(bufferedList.size==threshold){
        //输出到外部系统
        bufferedList.foreach(println(_))
        println("-----------------")
        bufferedList.clear()
      }
    }

    //定义一个本地变量列表
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      //清空状态
      eventState.clear()
      for(data<-bufferedList){
        eventState.add(data)
      }
    }
    override def initializeState(context: FunctionInitializationContext): Unit = {
      eventState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Event]("buffered-list",classOf[Event]))
      if(context.isRestored){
        import scala.collection.convert.ImplicitConversions._
        for(data <- eventState.get()){
          bufferedList += data
        }
      }
    }
  }
}
