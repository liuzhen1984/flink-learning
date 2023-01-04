package com.fortinet.flink.source

import com.fortinet.flink.Event
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

class ParallelClickSource extends ParallelSourceFunction[Event]{
  var isRunning = true;

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val users = Array("Mary", "Alice", "Bob", "Cary", "Jack", "Lucy")
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3")
    val random = new Random()
    while (isRunning) {
      val event = Event(users(random.nextInt(users.length)),urls(random.nextInt(urls.length)),System.currentTimeMillis())
      ctx.collect(event)
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
