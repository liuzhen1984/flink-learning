package com.fortinet.flink.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object JoinListStateTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val stream1 = env.fromElements(
    ("a","stream-1",1000L),
    ("a","stream-2",2000L)
  )
    val stream2 = env.fromElements(
      ("a", "stream-2", 3000L),
      ("b", "stream-2", 4000L)
    )
      stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .process(new TwoStreamJoin)
      .print()

  env.execute()

  class TwoStreamJoin extends CoProcessFunction[(String,String,Long),(String,String,Long),String] {
    lazy val listState1:ListState[(String,String,Long)] = getRuntimeContext.getListState(new ListStateDescriptor[(String,String,Long)]("stream1-list-1",classOf[(String,String,Long)]))
    lazy val listState2:ListState[(String,String,Long)] = getRuntimeContext.getListState(new ListStateDescriptor[(String,String,Long)]("stream1-list-2",classOf[(String,String,Long)])
    )
    override def processElement1(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      listState1.add(value)
      //遍历另外一个list state中数据做join
      import scala.collection.convert.ImplicitConversions._ //为了解决java中的iterable 转换成scala中的
      for(s2 <- listState2.get()){
          out.collect(value+"=>"+s2)
      }
    }

    override def processElement2(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      listState2.add(value)
      //遍历另外一个list state中数据做join
      import scala.collection.convert.ImplicitConversions._
      for (s1 <- listState1.get()) {
        out.collect(value + "=>" + s1)
      }
    }
  }

}
