package com.fortinet.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.IOUtils

import java.io.BufferedReader
import java.net.{InetSocketAddress, Socket}
import scala.tools.jline_embedded.internal.InputStreamReader

object SocketSource{
  def apply(host:String,port:Int): SocketSource ={
    return new SocketSource(host,port)
  }
}

class SocketSource(val host:String, val port:Int) extends SourceFunction[String] {
  var running = true
  val CONNECTION_TIMEOUT_TIME = 0;
  var socket:Socket = null


  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    var buffer = new StringBuffer()
    while (running) {
      socket = new Socket()
      socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_TIME)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      val cbuf = new Array[Char](8192)
      var bytesRead = 0;
      while(running && (bytesRead = reader.read(cbuf)) != -1 ) {
        buffer.append(cbuf,0,bytesRead)
        sourceContext.collect(buffer.toString)
        buffer.delete(0,buffer.length())
      }

      if(running){
        Thread.sleep(100)
      }
      /**
       *  try (BufferedReader reader =
       *  new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
       *
       *  char[] cbuf = new char[8192];
       *  int bytesRead;
       *  while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
       *  buffer.append(cbuf, 0, bytesRead);

       *  ctx.collect(record);
       *  buffer.delete(0, delimPos + delimiter.length());
       *  }
       *  }
       *  }
       */
    }
  }

  override def cancel(): Unit = {
    this.running = false
    if(socket!=null){
      IOUtils.closeSocket(socket)
      socket=null;
    }
  }
}