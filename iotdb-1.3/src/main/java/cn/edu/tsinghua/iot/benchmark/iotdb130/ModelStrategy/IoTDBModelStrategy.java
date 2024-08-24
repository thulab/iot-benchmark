/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.iotdb130.ModelStrategy;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

public abstract class IoTDBModelStrategy {
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected final DBConfig dbConfig;

  public IoTDBModelStrategy(DBConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public abstract Session buildSession(List<String> hostUrls);

  public abstract void registerSchema(
      Map<Session, List<TimeseriesSchema>> sessionListMap, List<DeviceSchema> schemaList)
      throws TsdbException;

  public abstract Tablet createTablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<Tablet.ColumnType> columnTypes,
      int maxRowNumber);

  public abstract String getDeviceId(DeviceSchema schema);

  public abstract String addSelectClause();

  public abstract String addPath(DeviceSchema deviceSchema);

  public abstract String addFromClause(List<DeviceSchema> devices, StringBuilder builder);

  public abstract void sessionInsertImpl(Session session, Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void sessionCleanupImpl(Session session)
      throws IoTDBConnectionException, StatementExecutionException;

  public abstract void genTablet(
      List<Tablet.ColumnType> columnTypes, List<Sensor> sensors, IBatch batch);
}
