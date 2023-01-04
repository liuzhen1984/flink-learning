package com.fortinet.flink.source

import com.fortinet.flink.Event
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class ClickSource extends SourceFunction[Event]{
  var isRunning = true;

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    val users = Array("Mary", "Alice", "Bob", "Cary", "Jack", "Lucy")
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3","./login","./pay","./buy")
    val random = new Random()
    while (isRunning) {
      val event = Event(users(random.nextInt(users.length)),urls(random.nextInt(urls.length)),System.currentTimeMillis())
//      ctx.collectWithTimestamp(event,event.timestamp)
//      ctx.emitWatermark(new Watermark(event.timestamp-1))

      ctx.collect(event)
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
