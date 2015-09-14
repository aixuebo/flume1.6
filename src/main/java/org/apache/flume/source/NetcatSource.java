/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A netcat-like source that listens on a given port and turns each line of text
 * into an event.
 * </p>
 * <p>
 * This source, primarily built for testing and exceedingly simple systems, acts
 * like <tt>nc -k -l [host] [port]</tt>. In other words, it opens a specified
 * port and listens for data. The expectation is that the supplied data is
 * newline separated text. Each line of text is turned into a Flume event and
 * sent via the connected channel.
 * </p>
 * <p>
 * Most testing has been done by using the <tt>nc</tt> client but other,
 * similarly implemented, clients should work just fine.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>max-line-length</tt></td>
 * <td>The maximum # of chars a line can be per event (including newline).</td>
 * <td>Number of UTF-8 characters / int</td>
 * <td>512</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class NetcatSource extends AbstractSource implements Configurable,
    EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(NetcatSource.class);

  private String hostName;
  private int port;
  private int maxLineLength;
  private boolean ackEveryEvent;//确认每一个事件都要接收ok字符串,返回到发送者这边

  private CounterGroup counterGroup;
  private ServerSocketChannel serverSocket;
  private AtomicBoolean acceptThreadShouldStop;
  private Thread acceptThread;//因为start方法要退出,不然其他方法无法执行,但是服务端socket又不能退出,因此重新启动一个线程,专门开启服务socket,用于长期监听连接进来
  private ExecutorService handlerService;

  public NetcatSource() {
    super();

    port = 0;
    counterGroup = new CounterGroup();
    acceptThreadShouldStop = new AtomicBoolean(false);
  }

  @Override
  public void configure(Context context) {
    String hostKey = NetcatSourceConfigurationConstants.CONFIG_HOSTNAME;
    String portKey = NetcatSourceConfigurationConstants.CONFIG_PORT;
    String ackEventKey = NetcatSourceConfigurationConstants.CONFIG_ACKEVENT;

    Configurables.ensureRequiredNonNull(context, hostKey, portKey);

    hostName = context.getString(hostKey);
    port = context.getInteger(portKey);
    ackEveryEvent = context.getBoolean(ackEventKey, true);
    maxLineLength = context.getInteger(
        NetcatSourceConfigurationConstants.CONFIG_MAX_LINE_LENGTH,
        NetcatSourceConfigurationConstants.DEFAULT_MAX_LINE_LENGTH);
  }

  @Override
  public void start() {

    logger.info("Source starting");

    counterGroup.incrementAndGet("open.attempts");

    handlerService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("netcat-handler-%d").build());

    try {
    	//信息要发往远程的ip和端口
      SocketAddress bindPoint = new InetSocketAddress(hostName, port);

      //在ip和端口下建立一个服务器socket
      serverSocket = ServerSocketChannel.open();
      serverSocket.socket().setReuseAddress(true);
      serverSocket.socket().bind(bindPoint);

      logger.info("Created serverSocket:{}", serverSocket);
    } catch (IOException e) {
      counterGroup.incrementAndGet("open.errors");
      logger.error("Unable to bind to socket. Exception follows.", e);
      throw new FlumeException(e);
    }

    //因为start方法要退出,不然其他方法无法执行,但是服务端socket又不能退出,因此重新启动一个线程,专门开启服务socket,用于长期监听连接进来
    //1.测试内部类是可以使用外部类的属性的
    //2.如果内部类是static静态的,则是不能使用外部类的属性的,因此需要外部类传递信息给静态AcceptHandler类
    AcceptHandler acceptRunnable = new AcceptHandler(maxLineLength);
    acceptThreadShouldStop.set(false);
    acceptRunnable.counterGroup = counterGroup;
    acceptRunnable.handlerService = handlerService;
    acceptRunnable.shouldStop = acceptThreadShouldStop;
    acceptRunnable.ackEveryEvent = ackEveryEvent;
    acceptRunnable.source = this;
    acceptRunnable.serverSocket = serverSocket;

    acceptThread = new Thread(acceptRunnable);

    acceptThread.start();

    logger.debug("Source started");
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Source stopping");

    acceptThreadShouldStop.set(true);

    if (acceptThread != null) {//关闭socket服务器线程
      logger.debug("Stopping accept handler thread");

      while (acceptThread.isAlive()) {
        try {
          logger.debug("Waiting for accept handler to finish");
          acceptThread.interrupt();
          acceptThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug("Interrupted while waiting for accept handler to finish");
          Thread.currentThread().interrupt();
        }
      }

      logger.debug("Stopped accept handler thread");
    }

    //关闭socket服务器
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Unable to close socket. Exception follows.", e);
        return;
      }
    }

    //关闭线程池
    if (handlerService != null) {
      handlerService.shutdown();

      logger.debug("Waiting for handler service to stop");

      // wait 500ms for threads to stop
      try {
        handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for netcat handler service to stop");
        Thread.currentThread().interrupt();
      }

      if (!handlerService.isShutdown()) {
        handlerService.shutdownNow();
      }

      logger.debug("Handler service stopped");
    }

    logger.debug("Source stopped. Event metrics:{}", counterGroup);
    super.stop();
  }

  /**
   * 因为start方法要退出,不然其他方法无法执行,但是服务端socket又不能退出,因此重新启动一个线程,专门开启服务socket,用于长期监听连接进来
   * 1.测试内部类是可以使用外部类的属性的
   * 2.如果内部类是static静态的,则是不能使用外部类的属性的,因此需要外部类传递信息给静态AcceptHandler类
   */
  private static class AcceptHandler implements Runnable {

    private ServerSocketChannel serverSocket;
    private CounterGroup counterGroup;
    private ExecutorService handlerService;
    private EventDrivenSource source;
    private AtomicBoolean shouldStop;
    private boolean ackEveryEvent;

    private final int maxLineLength;

    public AcceptHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting accept handler");

      //循环获取监听
      while (!shouldStop.get()) {
        try {
        	//说明有请求到了,则将请求转换到一个线程中去执行,让其可以继续不阻塞的接收其他任务
          SocketChannel socketChannel = serverSocket.accept();
          //当socket服务器接收到请求的时候,创建该线程真正去执行该请求
          NetcatSocketHandler request = new NetcatSocketHandler(maxLineLength);

          request.socketChannel = socketChannel;
          request.counterGroup = counterGroup;
          request.source = source;
          request.ackEveryEvent = ackEveryEvent;

          handlerService.submit(request);

          counterGroup.incrementAndGet("accept.succeeded");
        } catch (ClosedByInterruptException e) {
          // Parent is canceling us.
        } catch (IOException e) {
          logger.error("Unable to accept connection. Exception follows.", e);
          counterGroup.incrementAndGet("accept.failed");
        }
      }

      logger.debug("Accept handler exiting");
    }
  }

  /**
   * 当socket服务器接收到请求的时候,创建该线程真正去执行该请求
   * 1.测试内部类是可以使用外部类的属性的
   * 2.如果内部类是static静态的,则是不能使用外部类的属性的,因此需要外部类传递信息给静态AcceptHandler类
   */
  private static class NetcatSocketHandler implements Runnable {

    private Source source;
    private CounterGroup counterGroup;
    private SocketChannel socketChannel;//客户端和服务端交互的socket对象
    private boolean ackEveryEvent;

    private final int maxLineLength;

    public NetcatSocketHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting connection handler");
      Event event = null;

      try {
        Reader reader = Channels.newReader(socketChannel, "utf-8");
        Writer writer = Channels.newWriter(socketChannel, "utf-8");
        CharBuffer buffer = CharBuffer.allocate(maxLineLength);
        buffer.flip(); // flip() so fill() sees buffer as initially empty

        while (true) {
          // this method blocks until new data is available in the socket 因为是接收到数据了,才会触发该run,因此一定reader是有内容的,将内容发送到buffer中
          int charsRead = fill(buffer, reader);//返回读取的字节数
          logger.debug("Chars read = {}", charsRead);

          // attempt to process all the events in the buffer 将buffer的内容进行处理,并且将结果信息写入到输出流writer中
          int eventsProcessed = processEvents(buffer, writer);//从输出流中处理了多少个\n分隔的事件
          logger.debug("Events processed = {}", eventsProcessed);

          if (charsRead == -1) {//读取字节数为-1,说明读取到结尾了,停止运行该线程
            // if we received EOF before last event processing attempt, then we
            // have done everything we can
            break;
          } else if (charsRead == 0 && eventsProcessed == 0) {
            if (buffer.remaining() == buffer.capacity()) {
              // If we get here it means:
              // 1. Last time we called fill(), no new chars were buffered
              // 2. After that, we failed to process any events => no newlines
              // 3. The unread data in the buffer == the size of the buffer
              // Therefore, we are stuck because the client sent a line longer
              // than the size of the buffer. Response: Drop the connection.
              logger.warn("Client sent event exceeding the maximum length");
              counterGroup.incrementAndGet("events.failed");
              writer.write("FAILED: Event exceeds the maximum length (" +
                  buffer.capacity() + " chars, including newline)\n");
              writer.flush();
              break;
            }
          }
        }

        socketChannel.close();

        //计算成功完成的次数
        counterGroup.incrementAndGet("sessions.completed");
      } catch (IOException e) {
    	  //计算出错的次数
        counterGroup.incrementAndGet("sessions.broken");
      }

      logger.debug("Connection handler exiting");
    }

    /**
     * <p>Consume some number of events from the buffer into the system.</p>
     *
     * Invariants (pre- and post-conditions): <br/>
     *   buffer should have position @ beginning of unprocessed data. <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * @param buffer The buffer containing data to process
     * @param writer The channel back to the client
     * @return number of events successfully processed
     * @throws IOException
     * 将buffer的内容进行处理,并且将结果信息写入到输出流writer中
     * 
     * 返回从输出流中处理了多少个\n分隔的事件
     */
    private int processEvents(CharBuffer buffer, Writer writer)
        throws IOException {

    	//从输出流中处理了多少个\n分隔的事件
      int numProcessed = 0;

      boolean foundNewLine = true;
      while (foundNewLine) {
        foundNewLine = false;

        int limit = buffer.limit();
        for (int pos = buffer.position(); pos < limit; pos++) {//一直循环到最后,直到发现\n为止,表示遇到换行了
          if (buffer.get(pos) == '\n') {//当遇到换行的时候

            // parse event body bytes out of CharBuffer
            buffer.limit(pos); // temporary limit 在buffer中设置属于该\n字符的位置
            ByteBuffer bytes = Charsets.UTF_8.encode(buffer);//将属于\n之前的数据保存,用UTF-8编码转换成字节数组
            buffer.limit(limit); // restore limit 重新设置buffer到最后位置,使下次即可继续循环

            // build event object 处理本次的信息 都设置到body中
            byte[] body = new byte[bytes.remaining()];//bytes.remaining()返回bytes中可用的字节总长度
            bytes.get(body);//将可用的字节信息转化成字节数组,写入到body中
            Event event = EventBuilder.withBody(body);//将字节数组转化成事件

            // process event 让该source对应的channel处理该事件
            ChannelException ex = null;
            try {
              source.getChannelProcessor().processEvent(event);
            } catch (ChannelException chEx) {
              ex = chEx;
            }

            if (ex == null) {
            	//如果处理后没有异常,则累加处理事件次数
              counterGroup.incrementAndGet("events.processed");
              numProcessed++;
              if (true == ackEveryEvent) {//向输出流中写入OK
                writer.write("OK\n");
              }
            } else {//说明有问题,因此计数失败事件处理次数
              counterGroup.incrementAndGet("events.failed");
              logger.warn("Error processing event. Exception follows.", ex);
              //向输出流中写入失败原因
              writer.write("FAILED: " + ex.getMessage() + "\n");
            }
            writer.flush();

            // advance position after data is consumed
            buffer.position(pos + 1); // skip newline 跳过\n这个字符
            foundNewLine = true;
            break;
          }
        }

      }

      return numProcessed;
    }

    /**
     * <p>Refill the buffer read from the socket.</p>
     *
     * Preconditions: <br/>
     *   buffer should have position @ beginning of unprocessed data. <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * Postconditions: <br/>
     *   buffer should have position @ beginning of buffer (pos=0). <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * Note: this method blocks on new data arriving.
     *
     * @param buffer The buffer to fill
     * @param reader The Reader to read the data from
     * @return number of characters read
     * @throws IOException
     * 因为是接收到数据了,才会触发该run,因此一定reader是有内容的,将内容发送到buffer中
     * 
     * 返回读取的字节数
     */
    private int fill(CharBuffer buffer, Reader reader)
        throws IOException {

      // move existing data to the front of the buffer
      buffer.compact();

      // pull in as much data as we can from the socket
      //将reader数据读入到buffer中,返回读取的字节数
      int charsRead = reader.read(buffer);
      counterGroup.addAndGet("characters.received", Long.valueOf(charsRead));

      // flip so the data can be consumed
      buffer.flip();

      return charsRead;
    }

  }
}
