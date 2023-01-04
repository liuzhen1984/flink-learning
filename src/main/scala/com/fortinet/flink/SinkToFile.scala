package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.time.Duration

object SinkToFile {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.addSource(new ClickSource())
    val sink = FileSink.forRowFormat(new Path("./output"),new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.builder()
        .withRolloverInterval(Duration.ofMillis(15))
        .withInactivityInterval(Duration.ofMillis(15))
        .withMaxPartSize(MemorySize.ofMebiBytes(1024*1024*1024)).build()
      )
      .build()
     stream.map(_.toString).sinkTo(sink).setParallelism(4)
    env.execute()
  }

}

