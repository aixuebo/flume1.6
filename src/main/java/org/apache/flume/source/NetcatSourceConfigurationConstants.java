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

public class NetcatSourceConfigurationConstants {

  /**
   * Hostname to bind to.
   */
  public static final String CONFIG_HOSTNAME = "bind";

  /**
   * Port to bind to.
   */
  public static final String CONFIG_PORT = "port";


  /**
   * Ack every event received with an "OK" back to the sender
   * 确认每一个事件都要接收ok字符串,返回到发送者这边
   * 该属性对应的value是boolean类型的
   */
  public static final String CONFIG_ACKEVENT = "ack-every-event";

  /**
   * Maximum line length per event.
   * 最大的传输每行字符长度
   */
  public static final String CONFIG_MAX_LINE_LENGTH = "max-line-length";
  public static final int DEFAULT_MAX_LINE_LENGTH = 512;

}
