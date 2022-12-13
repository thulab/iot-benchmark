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

package cn.edu.tsinghua.iot.benchmark.measurement.persistence;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.csv.CSVRecorder;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.iotdb.IotdbRecorder;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.mysql.MySqlRecorder;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.none.NoneRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceFactory.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public TestDataPersistence getPersistence() {
    switch (config.getTEST_DATA_PERSISTENCE()) {
      case Constants.TDP_NONE:
        return new NoneRecorder();
      case Constants.TDP_IOTDB:
        return new IotdbRecorder();
      case Constants.TDP_MYSQL:
        return new MySqlRecorder();
      case Constants.TDP_CSV:
        return new CSVRecorder();
      default:
        LOGGER.error(
            "unsupported test data persistence way: {}, use NoneRecorder",
            config.getTEST_DATA_PERSISTENCE());
        return new NoneRecorder();
    }
  }
}
