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

package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

public class DBFactory {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  public DBFactory() {}

  /** Get database according to DB_SWITCH */
  public IDatabase getDatabase(DBConfig dbConfig) throws SQLException {
    String dbClass = "";
    try {
      switch (dbConfig.getDB_SWITCH()) {
          // IoTDB 0.14
        case DB_IOT_014_JDBC:
          dbClass = Constants.IOTDB014_JDBC_CLASS;
          break;
        case DB_IOT_014_SESSION_BY_TABLET:
        case DB_IOT_014_SESSION_BY_RECORD:
        case DB_IOT_014_SESSION_BY_RECORDS:
          if (config.isIS_ALL_NODES_VISIBLE()) {
            dbClass = Constants.IOTDB014_ROUNDROBIN_SESSION_CLASS;
          } else {
            dbClass = Constants.IOTDB014_SESSION_CLASS;
          }
          break;
          // IoTDB 0.13
        case DB_IOT_013_JDBC:
          dbClass = Constants.IOTDB013_JDBC_CLASS;
          break;
        case DB_IOT_013_SESSION_BY_TABLET:
        case DB_IOT_013_SESSION_BY_RECORD:
        case DB_IOT_013_SESSION_BY_RECORDS:
          if (config.isIS_ALL_NODES_VISIBLE()) {
            dbClass = Constants.IOTDB013_ROUNDROBIN_SESSION_CLASS;
          } else {
            dbClass = Constants.IOTDB013_SESSION_CLASS;
          }
          break;
          // IoTDB 0.12
        case DB_IOT_012_JDBC:
          dbClass = Constants.IOTDB012_JDBC_CLASS;
          break;
        case DB_IOT_012_SESSION_BY_TABLET:
        case DB_IOT_012_SESSION_BY_RECORD:
        case DB_IOT_012_SESSION_BY_RECORDS:
          if (config.isIS_ALL_NODES_VISIBLE()) {
            dbClass = Constants.IOTDB012_ROUNDROBIN_SESSION_CLASS;
          } else {
            dbClass = Constants.IOTDB012_SESSION_CLASS;
          }
          break;
          // IoTDB 0.11
        case DB_IOT_011_JDBC:
          dbClass = Constants.IOTDB011_JDBC_CLASS;
          break;
        case DB_IOT_011_SESSION_POOL:
          dbClass = Constants.IOTDB011_SESSION_POOL_CLASS;
          break;
        case DB_IOT_011_SESSION:
          dbClass = Constants.IOTDB011_SESSION_CLASS;
          break;
          // IoTDB 0.10
        case DB_IOT_010_JDBC:
          dbClass = Constants.IOTDB010_JDBC_CLASS;
          break;
        case DB_IOT_010_SESSION:
          dbClass = Constants.IOTDB010_SESSION_CLASS;
          break;
          // IoTDB 0.9
        case DB_IOT_09_JDBC:
          dbClass = Constants.IOTDB009_JDBC_CLASS;
          break;
        case DB_IOT_09_SESSION:
          dbClass = Constants.IOTDB009_SESSION_CLASS;
          break;
        case DB_INFLUX:
          dbClass = Constants.INFLUXDB_CLASS;
          break;
        case DB_INFLUX_2:
          dbClass = Constants.INFLUXDB2_CLASS;
          break;
        case DB_KAIROS:
          dbClass = Constants.KAIROSDB_CLASS;
          break;
        case DB_OPENTS:
          dbClass = Constants.OPENTSDB_CLASS;
          break;
        case DB_TIMESCALE:
          dbClass = Constants.TIMESCALEDB_CLASS;
          break;
        case DB_TDENGINE:
          dbClass = Constants.TDENGINE_CLASS;
          break;
        case DB_TDENGINE_3:
          dbClass = Constants.TDENGINE3_CLASS;
          break;
        case DB_FAKE:
          dbClass = Constants.FAKEDB_CLASS;
          break;
        case DB_QUESTDB:
          dbClass = Constants.QUESTDB_CLASS;
          break;
        case DB_MSSQLSERVER:
          dbClass = Constants.MSSQLSERVER_CLASS;
          break;
        case DB_VICTORIAMETRICS:
          dbClass = Constants.VICTORIAMETRICS;
          break;
        case DB_SQLITE:
          dbClass = Constants.SQLITE_CLASS;
          break;
        case DB_PIARCHIVE:
          dbClass = Constants.PI_ARCHIVE_CLASS;
          break;
        case DB_IginX:
          dbClass = Constants.IGINX_CLASS;
          break;
        default:
          throw new SQLException("didn't support this database");
      }
      Class<?> databaseClass = Class.forName(dbClass);
      Constructor<?> constructor = databaseClass.getConstructor(DBConfig.class);
      return (IDatabase) constructor.newInstance(dbConfig);
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      e.printStackTrace();
    }
    throw new SQLException("init database " + dbConfig.getDB_SWITCH() + " failed");
  }
}
