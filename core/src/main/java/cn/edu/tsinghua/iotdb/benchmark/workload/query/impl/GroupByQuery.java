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

public class GroupByQuery extends RangeQuery {
  /** use startTimestamp to be the segment start time */
  private String aggFun;

  private long granularity;

  public String getAggFun() {
    return aggFun;
  }

  public long getGranularity() {
    return granularity;
  }

  public GroupByQuery(
      List<DeviceSchema> deviceSchema,
      long startTimestamp,
      long endTimestamp,
      String aggFun,
      long granularity) {
    super(deviceSchema, startTimestamp, endTimestamp);
    this.aggFun = aggFun;
    this.granularity = granularity;
  }

  /**
   * get attributes of query
   *
   * @return
   */
  @Override
  public StringBuilder getQueryAttrs() {
    StringBuilder stringBuilder = super.getQueryAttrs();
    stringBuilder.append(" aggFun=").append(aggFun);
    stringBuilder.append(" granularity=").append(granularity);
    return stringBuilder;
  }
}
