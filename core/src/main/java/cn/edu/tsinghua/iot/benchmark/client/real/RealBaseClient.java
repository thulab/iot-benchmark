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

package cn.edu.tsinghua.iot.benchmark.client.real;

import cn.edu.tsinghua.iot.benchmark.client.DataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class RealBaseClient extends DataClient implements Runnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RealBaseClient.class);

  public RealBaseClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
  }

  @Override
  protected void initDBWrappers() {
    super.initDBWrappers();
    this.totalLoop = this.dataWorkLoad.getBatchNumber();
    if (!config.isIS_SENSOR_TS_ALIGNMENT()) {
      this.totalLoop *= config.getSENSOR_NUMBER();
    }
  }
}
