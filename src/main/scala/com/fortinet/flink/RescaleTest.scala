package com.fortinet.flink

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object RescaleTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        for ( i <- 0 to 7 ){
          if(getRuntimeContext.getIndexOfThisSubtask == ((i+1)%2)){
              ctx.collect(i+1)
          }
        }
      }

      override def cancel(): Unit = ???
    }).setParallelism(2)
    stream.rescale.print("rescale").setParallelism(4)
    env.execute()
  }

}

