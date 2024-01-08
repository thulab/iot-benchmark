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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml.IntervalXml;

public class Interval {
  private long writeIntervalLower;
  private long writeIntervalUpper;
  private long timeIntervalLower;
  private long timeIntervalUpper;

  public Interval(IntervalXml intervalXml) {
    this.writeIntervalLower = intervalXml.getWriteInterval().getLower();
    this.writeIntervalUpper = intervalXml.getWriteInterval().getUpper();
    this.timeIntervalLower = intervalXml.getTimeInterval().getLower();
    this.timeIntervalUpper = intervalXml.getTimeInterval().getUpper();
  }

  public long getWriteIntervalLower() {
    return writeIntervalLower;
  }

  public long getWriteIntervalUpper() {
    return writeIntervalUpper;
  }

  public long getTimeIntervalLower() {
    return timeIntervalLower;
  }

  public long getTimeIntervalUpper() {
    return timeIntervalUpper;
  }
}
