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

package cn.edu.tsinghua.iot.benchmark.conf;

import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;

/** The constants of system */
public class Constants {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  public static final long START_TIMESTAMP =
      TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
  public static final String CONSOLE_PREFIX = "iot-benchmark>";

  public static final String BENCHMARK_HOME = "BENCHMARK_HOME";
  public static final String BENCHMARK_CONF = "benchmark-conf";

  public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

  /** Properties when generate data */
  public static final String SCHEMA_PATH = "schema.txt";

  public static final String INFO_PATH = "info.txt";

  /** support test data persistence */
  public static final String TDP_NONE = "None";

  public static final String TDP_IOTDB = "IoTDB";
  public static final String TDP_MYSQL = "MySQL";
  public static final String TDP_CSV = "CSV";

  /** device and storage group assignment */
  public static final String MOD_SG_ASSIGN_MODE = "mod";

  public static final String HASH_SG_ASSIGN_MODE = "hash";
  public static final String DIV_SG_ASSIGN_MODE = "div";

  public static final String IOTDB130_REST_CLASS = "cn.edu.tsinghua.iot.benchmark.iotdb130.RestAPI";
  public static final String IOTDB130_JDBC_CLASS = "cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB";
  public static final String IOTDB130_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDBSession";
  public static final String IOTDB130_ROUNDROBIN_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDBClusterSession";

  public static final String IOTDB110_JDBC_CLASS = "cn.edu.tsinghua.iot.benchmark.iotdb110.IoTDB";
  public static final String IOTDB110_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb110.IoTDBSession";
  public static final String IOTDB110_ROUNDROBIN_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb110.IoTDBClusterSession";

  public static final String IOTDB100_JDBC_CLASS = "cn.edu.tsinghua.iot.benchmark.iotdb100.IoTDB";
  public static final String IOTDB100_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb100.IoTDBSession";
  public static final String IOTDB100_ROUNDROBIN_SESSION_CLASS =
      "cn.edu.tsinghua.iot.benchmark.iotdb100.IoTDBClusterSession";

  public static final String INFLUXDB_CLASS = "cn.edu.tsinghua.iot.benchmark.influxdb.InfluxDB";
  public static final String INFLUXDB2_CLASS = "cn.edu.tsinghua.iot.benchmark.influxdb2.InfluxDB";
  public static final String CNOSDB_CLASS = "cn.edu.tsinghua.iot.benchmark.cnosdb.CnosDB";
  public static final String FAKEDB_CLASS = "cn.edu.tsinghua.iot.benchmark.tsdb.fakedb.FakeDB";
  public static final String KAIROSDB_CLASS = "cn.edu.tsinghua.iot.benchmark.kairosdb.KairosDB";
  public static final String OPENTSDB_CLASS = "cn.edu.tsinghua.iot.benchmark.opentsdb.OpenTSDB";
  public static final String TIMESCALEDB_CLASS =
      "cn.edu.tsinghua.iot.benchmark.timescaledb.TimescaleDB";
  public static final String TIMESCALEDB_CLUSTER_CLASS =
      "cn.edu.tsinghua.iot.benchmark.timescaledbCluster.TimescaleDB";

  public static final String TDENGINE_CLASS = "cn.edu.tsinghua.iot.benchmark.tdengine.TDengine";
  public static final String TDENGINE3_CLASS = "cn.edu.tsinghua.iot.benchmark.tdengine3.TDengine";
  public static final String MSSQLSERVER_CLASS =
      "cn.edu.tsinghua.iot.benchmark.mssqlserver.MsSQLServerDB";
  public static final String VICTORIAMETRICS =
      "cn.edu.tsinghua.iot.benchmark.victoriametrics.VictoriaMetrics";
  public static final String QUESTDB_CLASS = "cn.edu.tsinghua.iot.benchmark.questdb.QuestDB";
  public static final String SQLITE_CLASS = "cn.edu.tsinghua.iot.benchmark.sqlite.SqliteDB";
  public static final String PI_ARCHIVE_CLASS = "cn.edu.tsinghua.iot.benchmark.piarchive.PIArchive";
  public static final String IGINX_CLASS = "cn.edu.tsinghua.iot.benchmark.iginx.IginX";
  public static final String SELF_CHECK_CLASS = "cn.edu.tsinghua.iot.benchmark.tsdb.self.SelfCheck";
}
