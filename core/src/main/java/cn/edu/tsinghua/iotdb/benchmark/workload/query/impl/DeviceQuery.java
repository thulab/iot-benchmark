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

public class DeviceQuery extends Query {
  private DeviceSchema deviceSchema;
  private int offset = 0;
  private int limit = 500;

  public DeviceQuery() {}

  public DeviceQuery(DeviceSchema deviceSchema) {
    this.deviceSchema = deviceSchema;
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getOffset() {
    return offset;
  }

  public int getLimit() {
    return limit;
  }

  public DeviceQuery getQueryWithOffset(int offset) {
    DeviceQuery deviceQuery = new DeviceQuery(deviceSchema);
    deviceQuery.setLimit(limit);
    deviceQuery.setOffset(offset);
    return deviceQuery;
  }

  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("DeviceSchema=").append(deviceSchema);
    stringBuilder.append(" limit=").append(limit);
    stringBuilder.append(" offset=").append(offset);
    return stringBuilder;
  }
}
