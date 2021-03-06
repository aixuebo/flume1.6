/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * 该接口定义了key-value类型,以及传入一个事件,进行存储的函数
 */
public interface SequenceFileSerializer {

  Class<?> getKeyClass();

  Class<?> getValueClass();

  /**
   * Format the given event into zero, one or more SequenceFile records
   *
   * @param e
   *         event
   * @return a list of records corresponding to the given event
   * 将事件转化成一系列Record对象,最终该Record对象会被进行写入到HDFS中,注意这Record对象中的key-value是要被可以序列化的
   */
  Iterable<Record> serialize(Event e);

  /**
   * Knows how to construct this output formatter.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public SequenceFileSerializer build(Context context);
  }

  /**
   * A key-value pair making up a record in an HDFS SequenceFile
   */
  public static class Record {
    private final Object key;
    private final Object value;

    public Record(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    public Object getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }
  }

}
