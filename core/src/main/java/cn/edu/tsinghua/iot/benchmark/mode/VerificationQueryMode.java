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

package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.client.DataClient;
import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;

import java.util.Collections;

public class VerificationQueryMode extends BaseMode {

  @Override
  protected boolean preCheck() {
    return true;
  }

  @Override
  protected void postCheck() {
    finalMeasure(
        baseModeMeasurement,
        dataClients.stream().map(DataClient::getMeasurement),
        startTime,
        Collections.singletonList(Operation.VERIFICATION_QUERY));
  }
}
