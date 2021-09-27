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

package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;

/** 系统运行常量值 */
public class Constants {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  public static final long START_TIMESTAMP =
      TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
  public static final String CONSOLE_PREFIX = "IotDB-benchmark>";
  public static final String BENCHMARK_CONF = "benchmark-conf";

  public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

  /** Properties when generate data */
  public static final String SCHEMA_PATH = "schema.txt";

  public static final String INFO_PATH = "info.txt";

  /** name prefix of group */
  public static final String GROUP_NAME_PREFIX = "g_";
  /** name prefix of device */
  public static final String DEVICE_NAME_PREFIX = "d_";
  /** name prefix of sensor */
  public static final String SENSOR_NAME_PREFIX = "s_";

  /** support test data persistence */
  public static final String TDP_NONE = "None";

  public static final String TDP_IOTDB = "IoTDB";
  public static final String TDP_MYSQL = "MySQL";
  public static final String TDP_CSV = "CSV";

  /** device and storage group assignment */
  public static final String MOD_SG_ASSIGN_MODE = "mod";

  public static final String HASH_SG_ASSIGN_MODE = "hash";
  public static final String DIV_SG_ASSIGN_MODE = "div";

  public static final String IOTDB012_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDB";
  public static final String IOTDB012_SESSION_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDBSession";
  public static final String IOTDB012_ROUNDROBIN_SESSION_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDBClusterSession";

  public static final String IOTDB011_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB";
  public static final String IOTDB011_SESSION_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDBSession";
  public static final String IOTDB011_SESSION_POOL_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDBSessionPool";
  public static final String IOTDB010_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDB";
  public static final String IOTDB010_SESSION_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDBSession";
  public static final String IOTDB009_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDB";
  public static final String IOTDB009_SESSION_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDBSession";
  public static final String INFLUXDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.influxdb.InfluxDB";
  public static final String INFLUXDB2_CLASS = "cn.edu.tsinghua.iotdb.benchmark.influxdb2.InfluxDB";

  public static final String FAKEDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.tsdb.fakedb.FakeDB";
  public static final String KAIROSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.kairosdb.KairosDB";
  public static final String OPENTSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.opentsdb.OpenTSDB";
  public static final String TIMESCALEDB_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.timescaledb.TimescaleDB";
  public static final String TAOSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.taosdb.TaosDB";
  public static final String MSSQLSERVER_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.mssqlserver.MsSQLServerDB";
  public static final String VICTORIAMETRICS =
      "cn.edu.tsinghua.iotdb.benchmark.victoriametrics.VictoriaMetrics";
  public static final String QUESTDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.questdb.QuestDB";
  public static final String SQLITE_CLASS = "cn.edu.tsinghua.iotdb.benchmark.sqlite.SqliteDB";
  public static final String PI_ARCHIVE_CLASS =
      "cn.edu.tsinghua.iotdb.benchmark.piarchive.PIArchive";
}
