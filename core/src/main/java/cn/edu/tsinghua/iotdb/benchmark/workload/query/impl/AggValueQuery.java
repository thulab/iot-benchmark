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

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;

import java.util.List;

public class AggValueQuery extends AggRangeQuery {

  /**
   * AggValueQuery is aggregation query without time filter which means time range should cover the
   * whole time series, however some TSDBs require the time condition, in that case we use a large
   * time range to cover the whole time series. However this method still can not guarantee that the
   * series is fully covered.
   */
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());
  private static final long timeRangeConst =
      (config.getTIMESTAMP_PRECISION().equals("ns")) ? 3L : 1000L;
  private static final long END_TIME =
      (Constants.START_TIMESTAMP
              + config.getPOINT_STEP() * config.getBATCH_SIZE_PER_WRITE() * 1000L * timeRangeConst)
          * timeStampConst;

  public AggValueQuery(List<DeviceSchema> deviceSchema, String aggFun, double valueThreshold) {
    super(deviceSchema, Constants.START_TIMESTAMP * timeStampConst, END_TIME, aggFun);
    this.valueThreshold = valueThreshold;
  }

  public AggValueQuery(
      List<DeviceSchema> deviceSchema,
      long startTime,
      long endTime,
      String aggFun,
      double valueThreshold) {
    super(deviceSchema, startTime, endTime, aggFun);
    this.valueThreshold = valueThreshold;
  }

  public double getValueThreshold() {
    return valueThreshold;
  }

  private final double valueThreshold;

  private static long getTimestampConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }
}
