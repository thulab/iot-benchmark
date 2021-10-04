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

import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;

import java.util.List;

public class RangeQuery extends Query {

  private List<DeviceSchema> deviceSchema;
  private long startTimestamp;
  private long endTimestamp;
  private boolean desc = false;

  public RangeQuery() {}

  public RangeQuery(List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public void setDesc(boolean desc) {
    this.desc = desc;
  }

  public boolean isDesc() {
    return desc;
  }

  /**
   * get attributes of query
   *
   * @return
   */
  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("deviceSchema=").append(deviceSchema);
    stringBuilder.append(" startTimeStamp=").append(startTimestamp);
    stringBuilder.append(" endTimeStamp=").append(endTimestamp);
    stringBuilder.append(" desc=").append(desc);
    return stringBuilder;
  }
}
