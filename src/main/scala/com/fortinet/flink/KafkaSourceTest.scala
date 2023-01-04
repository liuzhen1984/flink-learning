package com.fortinet.flink

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

object KafkaSourceTest {

  def main(args: Array[String]) :Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","test-group")

    //https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/datastream/kafka/
    val source :KafkaSource[String] = KafkaSource.builder().setProperties(properties).setTopics("test").setValueOnlyDeserializer(new SimpleStringSchema()).build()
    val stream = env.fromSource(source,WatermarkStrategy.noWatermarks(),"kafkaSourceForTest")
    stream.print()
    env.execute()

  }


}
