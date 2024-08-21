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

import cn.edu.tsinghua.iot.benchmark.iotdb130.IoTDB;
import cn.edu.tsinghua.iot.benchmark.iotdb130.TimeseriesSchema;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeStrategy extends IoTDBModelStrategy {

    protected static Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());


    public TreeStrategy(DBConfig dbConfig) {
        super(dbConfig);
    }

    @Override
    public void registerDatabase(Map<Session, List<TimeseriesSchema>> sessionListMap) {
        Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());
        for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
            Session metaSession = pair.getKey();
            List<TimeseriesSchema> schemaList = pair.getValue();
            // get all storage groups
            Set<String> groups = new HashSet<>();
            for (TimeseriesSchema timeseriesSchema : schemaList) {
                DeviceSchema schema = timeseriesSchema.getDeviceSchema();
                synchronized (IoTDB.class) {
                    if (!storageGroups.contains(schema.getGroup())) {
                        groups.add(schema.getGroup());
                        storageGroups.add(schema.getGroup());
                    }
                }
            }
            // register storage groups
            for (String group : groups) {
                try {
                    metaSession.setStorageGroup(ROOT_SERIES_NAME + "." + group);
                    if (config.isTEMPLATE()) {
                        metaSession.setSchemaTemplate(config.getTEMPLATE_NAME(), ROOT_SERIES_NAME + "." + group);
                    }
                } catch (Exception e) {
                    handleRegisterException(e);
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
                .version(Version.V_1_0)
                .sqlDialect(dbConfig.getSQL_DIALECT())
                .build();
    }

    @Override
    public String getDeviceId(DeviceSchema schema) {
        return getDevicePath(schema);
    }

    @Override
    public Tablet createTablet(String insertTargetName, List<IMeasurementSchema> schemas, List<Tablet.ColumnType> columnTypes, int maxRowNumber) {
        return new Tablet(insertTargetName, schemas, maxRowNumber);
    }

    @Override
    public void sessionInsertImpl(Session session, Tablet tablet) throws IoTDBConnectionException, StatementExecutionException {
        if (config.isVECTOR()) {
            session.insertAlignedTablet(tablet);
        } else {
            session.insertTablet(tablet);
        }
    }

    @Override
    public void sessionCleanupImpl(Session session) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet dataSet = session.executeQueryStatement("show databases");
        while (dataSet.hasNext()) {
            session.executeNonQueryStatement(
                    "drop database " + dataSet.next().getFields().get(0).toString());
        }
    }

    //region private method
    /**
     * convert deviceSchema to the format
     *
     * @param deviceSchema
     * @return format, e.g. root.group_1.d_1
     */
    protected String getDevicePath(DeviceSchema deviceSchema) {
        // TODO: copy from IoTDB.getDevicePath, should think a better way
        StringBuilder name = new StringBuilder(ROOT_SERIES_NAME);
        name.append(".").append(deviceSchema.getGroup());
        for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
            name.append(".").append(pair.getValue());
        }
        name.append(".").append(deviceSchema.getDevice());
        return name.toString();
    }
}
