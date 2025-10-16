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

package cn.edu.tsinghua.iot.benchmark.iotdb200;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleNodeJDBCConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeJDBCConnection.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  protected static final String JDBC_URL = "jdbc:iotdb://%s:%s/";
  protected DBConfig dbConfig;

  private Connection[] connections;
  private AtomicInteger currConnectionIndex = new AtomicInteger(0);

  public SingleNodeJDBCConnection(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public void init() throws TsdbException {
    int nodeSize = 1;
    String[] urls;
    urls = new String[nodeSize];
    urls[0] = String.format(JDBC_URL, dbConfig.getHOST().get(0), dbConfig.getPORT().get(0));
    connections = new Connection[nodeSize];

    for (int i = 0; i < connections.length; i++) {
      try {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable =
            config.isENABLE_THRIFT_COMPRESSION();
        Properties info = new Properties();
        info.setProperty("user", dbConfig.getUSERNAME());
        info.setProperty("password", dbConfig.getPASSWORD());
        if (config.isUSE_SSL()) {
          info.setProperty("use_ssl", "true");
          info.setProperty("trust_store", config.getTRUST_STORE_PATH());
          info.setProperty("trust_store_pwd", config.getTRUST_STORE_PWD());
        }
        connections[i] = DriverManager.getConnection(urls[i], info);
      } catch (Exception e) {
        LOGGER.error("Initialize IoTDB failed because ", e);
        throw new TsdbException(e);
      }
    }
  }

  public void close() throws TsdbException {
    for (Connection connection : connections) {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close IoTDB connection because ", e);
          throw new TsdbException(e);
        }
      }
    }
  }

  public Connection getConnection() {
    return connections[currConnectionIndex.incrementAndGet() % connections.length];
  }
}
