package com.fortinet.flink

import org.apache.flink.api.common.functions.MapFunction

class CountFunction extends MapFunction[String,(String,Int)]{
  override def map(value: String): (String, Int) = {
    (value,1)
  }
}
