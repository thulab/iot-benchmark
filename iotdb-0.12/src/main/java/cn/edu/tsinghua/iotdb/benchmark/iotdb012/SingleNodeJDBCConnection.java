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

package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleNodeJDBCConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeJDBCConnection.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Connection[] connections;
  private AtomicInteger currConnectionIndex = new AtomicInteger(0);

  public SingleNodeJDBCConnection() {}

  public void init() throws TsdbException {
    int nodeSize = 1;
    String[] urls;
    if (config.isIS_ALL_NODES_VISIBLE()) {
      nodeSize = config.getHOST().size();
      urls = new String[nodeSize];
      List<String> clusterHosts = config.getHOST();
      for (int i = 0; i < nodeSize; i++) {
        String jdbcUrl = String.format(Constants.URL, config.getHOST().get(i), config.getPORT().get(i));
        urls[i] = jdbcUrl;
      }
    } else {
      urls = new String[nodeSize];
      urls[0] = String.format(Constants.URL, config.getHOST().get(0), config.getPORT().get(0));
    }
    connections = new Connection[nodeSize];

    for (int i = 0; i < connections.length; i++) {
      try {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable =
            config.isENABLE_THRIFT_COMPRESSION();
        connections[i] = DriverManager.getConnection(urls[i], Constants.USER, Constants.PASSWD);
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
