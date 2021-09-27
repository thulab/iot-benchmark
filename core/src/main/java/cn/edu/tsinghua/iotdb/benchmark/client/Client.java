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

package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class Client implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private final CountDownLatch countDownLatch;
  private final CyclicBarrier barrier;

  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  protected Measurement measurement;
  protected int clientThreadId;
  protected List<DBWrapper> dbWrappers = new ArrayList<>();

  public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    clientThreadId = id;
    measurement = new Measurement();
    initDBWrappers();
  }

  /**
   * Firstly init dbWrapper After all thread is finished(using barrier), then doTest After test,
   * count down latch
   */
  @Override
  public void run() {
    try {
      try {
        for (DBWrapper dbWrapper : dbWrappers) {
          dbWrapper.init();
        }
        // wait for that all clients start test simultaneously
        barrier.await();

        doTest();

      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        try {
          for (DBWrapper dbWrapper : dbWrappers) {
            dbWrapper.close();
          }
        } catch (TsdbException | SQLException e) {
          LOGGER.error("Close {} error: ", config.getDbConfig().getDB_SWITCH(), e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  public Measurement getMeasurement() {
    return measurement;
  }

  /** Do test */
  protected abstract void doTest();

  /** Init DBWrapper */
  protected void initDBWrappers() {
    dbWrappers.add(new DBWrapper(config.getDbConfig(), measurement));
    if (config.isIS_DOUBLE_WRITE()) {
      dbWrappers.add(new DBWrapper(config.getANOTHER_DBConfig(), measurement));
    }
  }
}
