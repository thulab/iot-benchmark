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
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestWithDefaultPathMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestWithDefaultPathMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean preCheck() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();
    recorder.saveTestConfig();
    List<DBConfig> dbConfigs = new ArrayList<>();
    dbConfigs.add(config.getDbConfig());
    if (config.isIS_DOUBLE_WRITE()) {
      dbConfigs.add(config.getANOTHER_DBConfig());
    }
    if (config.isIS_DELETE_DATA() && (!cleanUpData(dbConfigs))) {
      return false;
    }
    if (config.isCREATE_SCHEMA() && (!registerSchema())) {
      return false;
    }
    return true;
  }

  @Override
  protected void postCheck() {
    List<Operation> operations;
    if (config.isIS_POINT_COMPARISON()) {
      operations = Collections.singletonList(Operation.DEVICE_QUERY);
    } else {
      operations = Operation.getNormalOperation();
    }
    finalMeasure(
        baseModeMeasurement,
        dataClients.stream().map(DataClient::getMeasurement),
        startTime,
        operations);
  }
}
