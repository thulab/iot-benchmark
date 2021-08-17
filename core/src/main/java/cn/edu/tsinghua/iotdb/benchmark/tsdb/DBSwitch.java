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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum DBSwitch {
  DB_IOT_012_JDBC(DBType.IoTDB, DBVersion.IOTDB_012, DBInsertMode.INSERT_USE_JDBC),
  DB_IOT_012_SESSION_BY_TABLET(
      DBType.IoTDB, DBVersion.IOTDB_012, DBInsertMode.INSERT_USE_SESSION_TABLET),
  DB_IOT_012_SESSION_BY_RECORD(
      DBType.IoTDB, DBVersion.IOTDB_012, DBInsertMode.INSERT_USE_SESSION_RECORD),
  DB_IOT_012_SESSION_BY_RECORDS(
      DBType.IoTDB, DBVersion.IOTDB_012, DBInsertMode.INSERT_USE_SESSION_RECORDS),
  DB_IOT_011_JDBC(DBType.IoTDB, DBVersion.IOTDB_011, DBInsertMode.INSERT_USE_JDBC),
  DB_IOT_011_SESSION_POOL(DBType.IoTDB, DBVersion.IOTDB_011, DBInsertMode.INSERT_USE_SESSION_POOL),
  DB_IOT_011_SESSION(DBType.IoTDB, DBVersion.IOTDB_011, DBInsertMode.INSERT_USE_SESSION),
  DB_IOT_010_JDBC(DBType.IoTDB, DBVersion.IOTDB_010, DBInsertMode.INSERT_USE_JDBC),
  DB_IOT_010_SESSION(DBType.IoTDB, DBVersion.IOTDB_010, DBInsertMode.INSERT_USE_SESSION),
  DB_IOT_09_JDBC(DBType.IoTDB, DBVersion.IOTDB_09, DBInsertMode.INSERT_USE_JDBC),
  DB_IOT_09_SESSION(DBType.IoTDB, DBVersion.IOTDB_09, DBInsertMode.INSERT_USE_SESSION),
  DB_DOUBLE_IOT(DBType.DoubleIoTDB, null, null),
  DB_INFLUX(DBType.InfluxDB, null, null),
  DB_INFLUX_2(DBType.InfluxDB, DBVersion.InfluxDB_2, null),
  DB_OPENTS(DBType.OpenTSDB, null, null),
  DB_CTS(DBType.CTSDB, null, null),
  DB_KAIROS(DBType.KairosDB, null, null),
  DB_TIMESCALE(DBType.TimescaleDB, null, null),
  DB_FAKE(DBType.FakeDB, null, null),
  DB_TAOSDB(DBType.TaosDB, null, null),
  DB_QUESTDB(DBType.QuestDB, null, null),
  DB_MSSQLSERVER(DBType.MSSQLSERVER, null, null),
  DB_VICTORIAMETRICS(DBType.VictoriaMetrics, null, null),
  DB_SQLITE(DBType.SQLite, null, null);

  private static final Logger LOGGER = LoggerFactory.getLogger(DBSwitch.class);
  DBType type;
  DBVersion version;
  DBInsertMode insertMode;

  DBSwitch(DBType Type, DBVersion version, DBInsertMode insertMode) {
    this.type = Type;
    this.version = version;
    this.insertMode = insertMode;
  }

  public DBType getType() {
    return type;
  }

  public DBVersion getVersion() {
    return version;
  }

  public DBInsertMode getInsertMode() {
    return insertMode;
  }

  public static DBSwitch getDBType(String dbSwitch) {
    for (DBSwitch db : DBSwitch.values()) {
      if (db.toString().equals(dbSwitch)) {
        return db;
      }
    }
    DBSwitch db = DBSwitch.DB_IOT_012_SESSION_BY_TABLET;
    LOGGER.warn("Using default DBType: " + db.toString());
    return db;
  }

  @Override
  public String toString() {
    StringBuffer dbType = new StringBuffer(type.toString());
    if (version != null) {
      dbType.append("-").append(version.toString());
    }
    if (insertMode != null) {
      dbType.append("-").append(insertMode.toString());
    }
    return dbType.toString();
  }
}
