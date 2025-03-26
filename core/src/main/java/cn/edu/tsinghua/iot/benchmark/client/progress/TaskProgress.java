/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.client.progress;

import java.util.concurrent.atomic.AtomicReference;

public class TaskProgress {
  /** Thread name */
  private final AtomicReference<String> threadNameAtomic;
  /** Total number of loop */
  private volatile long totalLoop;
  /** Loop Index, using for loop and log */
  private volatile long loopIndex;

  public TaskProgress() {
    this.threadNameAtomic = new AtomicReference<>();
    this.totalLoop = 1L;
    this.loopIndex = 0L;
  }

  public void setThreadName(String threadName) {
    threadNameAtomic.set(threadName);
  }

  public String getThreadName() {
    return threadNameAtomic.get();
  }

  public void setTotalLoop(Long value) {
    totalLoop = value;
  }

  public long getLoopIndex() {
    return loopIndex;
  }

  public void resetLoopIndex() {
    loopIndex = 0L;
  }

  public void incrementLoopIndex() {
    loopIndex++;
  }

  public double getPercent() {
    if (totalLoop == 0) {
      return 0.00D;
    }
    return (double) loopIndex * 100.0D / totalLoop;
  }
}
