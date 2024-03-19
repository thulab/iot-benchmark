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

package cn.edu.tsinghua.iot.benchmark.client;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class SchemaClient implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaClient.class);

  protected static Config config = ConfigDescriptor.getInstance().getConfig();

  /** The id of client */
  protected final int clientThreadId;
  /** Tested DataBase */
  protected DBWrapper dbWrapper = null;
  /** Related Schema */
  protected final List<DeviceSchema> deviceSchemas;
  /** Related Schema Size */
  protected final int deviceSchemasSize;

  /** Control the end of client */
  private final CountDownLatch countDownLatch;

  private final CyclicBarrier barrier;
  /** result of register */
  private Boolean result = false;

  public SchemaClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    this.countDownLatch = countDownLatch;
    this.barrier = barrier;
    this.clientThreadId = id;
    this.deviceSchemas = MetaDataSchema.getInstance().getDeviceSchemaByClientId(clientThreadId);
    this.deviceSchemasSize = deviceSchemas.size();
    initDBWrappers();
  }

  /** Init DBWrapper */
  private void initDBWrappers() {
    List<DBConfig> dbConfigs = new ArrayList<>();
    dbConfigs.add(config.getDbConfig());
    if (config.isIS_DOUBLE_WRITE()) {
      dbConfigs.add(config.getANOTHER_DBConfig());
    }
    dbWrapper = new DBWrapper(dbConfigs);
  }

  @Override
  public void run() {
    try {
      try {
        if (dbWrapper != null) {
          dbWrapper.init();
        }
        // wait for that all dataClients start test simultaneously
        barrier.await();

        // register
        try {
          result = (null == dbWrapper.registerSchema(deviceSchemas));
        } catch (TsdbException e) {
          LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
          result = false;
        }
      } catch (Exception e) {
        LOGGER.error("Unexpected error: ", e);
      } finally {
        try {
          if (dbWrapper != null) {
            dbWrapper.close();
          }
        } catch (TsdbException e) {
          LOGGER.error("Close {} error: ", config.getDbConfig().getDB_SWITCH(), e);
        }
      }
    } finally {
      countDownLatch.countDown();
    }
  }

  public Measurement getMeasurement() {
    return dbWrapper.getMeasurement();
  }

  public Boolean getResult() {
    return result;
  }
}
