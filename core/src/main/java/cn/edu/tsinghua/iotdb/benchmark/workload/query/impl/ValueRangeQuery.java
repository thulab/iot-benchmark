/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;

import java.util.List;

public class ValueRangeQuery extends RangeQuery {

  private double valueThreshold;
  private boolean desc = false;

  public ValueRangeQuery(
      List<DeviceSchema> deviceSchema,
      long startTimestamp,
      long endTimestamp,
      double valueThreshold) {
    super(deviceSchema, startTimestamp, endTimestamp);
    this.valueThreshold = valueThreshold;
  }

  public double getValueThreshold() {
    return valueThreshold;
  }

  public void setDesc(boolean desc) {
    this.desc = desc;
  }

  public boolean isDesc() {
    return desc;
  }
}
