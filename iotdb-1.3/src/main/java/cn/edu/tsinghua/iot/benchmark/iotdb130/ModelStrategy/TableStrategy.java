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

import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

public class TableStrategy extends IoTDBModelStrategy {

    public TableStrategy(DBConfig dbConfig) {
        super(dbConfig);
    }

    @Override
    public void registerDatabase(Map<Session, List<TimeseriesSchema>> sessionListMap) {
        if (isDatabaseNotExist(metaSession).get()) {
            synchronized (databaseNotExist) {
                if (isDatabaseNotExist(metaSession).get()) {
                    try {
                        metaSession.executeNonQueryStatement(
                                "create database " + config.getDbConfig().getDB_NAME());
                    } catch (IoTDBConnectionException | StatementExecutionException e) {
                        LOGGER.error("Failed to create database:" + e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public Session buildSession(List<String> hostUrls) {
        return new Session.Builder()
                .nodeUrls(hostUrls)
                .username(dbConfig.getUSERNAME())
                .password(dbConfig.getPASSWORD())
                .enableRedirection(true)
                .database(dbConfig.getDB_NAME())
                .version(Version.V_1_0)
                .sqlDialect(dbConfig.getSQL_DIALECT())
                .build();
    }

    @Override
    public String getDeviceId(DeviceSchema schema) {
        return schema.getGroup() + "_table";
    }

    @Override
    public Tablet createTablet(String insertTargetName, List<IMeasurementSchema> schemas, List<Tablet.ColumnType> columnTypes, int maxRowNumber) {
        return new Tablet(insertTargetName, schemas, columnTypes, maxRowNumber);
    }

    @Override
    public void sessionInsertImpl(Session session, Tablet tablet) throws IoTDBConnectionException, StatementExecutionException {
        session.insertRelationalTablet(tablet);
    }

    @Override
    public void sessionCleanupImpl(Session session) throws IoTDBConnectionException, StatementExecutionException {
        session.executeNonQueryStatement(
                "drop database root." + config.getDbConfig().getDB_NAME() + ".**");
        session.executeNonQueryStatement("drop schema template " + config.getTEMPLATE_NAME());
    }
}
