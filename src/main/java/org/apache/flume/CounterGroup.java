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

package org.apache.flume;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used for counting events, collecting metrics, etc.
 */
public class CounterGroup {

  private String name;//组名
  //该组下每一个key对应的统计信息
  private HashMap<String, AtomicLong> counters;

  public CounterGroup() {
    counters = new HashMap<String, AtomicLong>();
  }

  //获取为name的key对应的value值
  public synchronized Long get(String name) {
    return getCounter(name).get();
  }

  //为name的key对应的value值累加1
  public synchronized Long incrementAndGet(String name) {
    return getCounter(name).incrementAndGet();
  }

  //为name的key对应的value值累加delta
  public synchronized Long addAndGet(String name, Long delta) {
    return getCounter(name).addAndGet(delta);
  }

  //合并两个统计组对象
  public synchronized void add(CounterGroup counterGroup) {
    synchronized (counterGroup) {
      for (Entry<String, AtomicLong> entry : counterGroup.getCounters()
          .entrySet()) {

        addAndGet(entry.getKey(), entry.getValue().get());
      }
    }
  }

  //为name的key设置value值
  public synchronized void set(String name, Long value) {
    getCounter(name).set(value);
  }

  //默认返回该name对应的值是1
  public synchronized AtomicLong getCounter(String name) {
    if (!counters.containsKey(name)) {
      counters.put(name, new AtomicLong());
    }

    return counters.get(name);
  }

  @Override
  public synchronized String toString() {
    return "{ name:" + name + " counters:" + counters + " }";
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized void setName(String name) {
    this.name = name;
  }

  //返回统计信息项目集合
  public synchronized HashMap<String, AtomicLong> getCounters() {
    return counters;
  }

  public synchronized void setCounters(HashMap<String, AtomicLong> counters) {
    this.counters = counters;
  }

}
