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

package cn.edu.tsinghua.iotdb.benchmark.client.generate;

import cn.edu.tsinghua.iotdb.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.operation.OperationController;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class SyntheticClient extends GenerateBaseClient {

  /** Control operation according to OPERATION_PROPORTION */
  private final OperationController operationController;

  public SyntheticClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier, new SyntheticDataWorkload(id));
    this.operationController = new OperationController(id);
  }

  /**
   * Do Operations
   *
   * @param actualDeviceFloor
   */
  @Override
  protected void doOperations(double actualDeviceFloor) {
    long start = 0;
    loop:
    for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
      Operation operation = operationController.getNextOperationType();
      if (config.getOP_INTERVAL() > 0) {
        start = System.currentTimeMillis();
      }
      switch (operation) {
        case INGESTION:
          if (!ingestionOperation(actualDeviceFloor)) {
            break loop;
          }
          break;
        case PRECISE_QUERY:
          try {
            dbWrapper.preciseQuery(syntheticWorkload.getPreciseQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case RANGE_QUERY:
          try {
            dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query because ", e);
          }
          break;
        case VALUE_RANGE_QUERY:
          try {
            dbWrapper.valueRangeQuery(syntheticWorkload.getValueRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do range query with value filter because ", e);
          }
          break;
        case AGG_RANGE_QUERY:
          try {
            dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query because ", e);
          }
          break;
        case AGG_VALUE_QUERY:
          try {
            dbWrapper.aggValueQuery(syntheticWorkload.getAggValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation query with value filter because ", e);
          }
          break;
        case AGG_RANGE_VALUE_QUERY:
          try {
            dbWrapper.aggRangeValueQuery(syntheticWorkload.getAggRangeValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query with value filter because ", e);
          }
          break;
        case GROUP_BY_QUERY:
          try {
            dbWrapper.groupByQuery(syntheticWorkload.getGroupByQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do group by query because ", e);
          }
          break;
        case LATEST_POINT_QUERY:
          try {
            dbWrapper.latestPointQuery(syntheticWorkload.getLatestPointQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do latest point query because ", e);
          }
          break;
        case RANGE_QUERY_ORDER_BY_TIME_DESC:
          try {
            dbWrapper.rangeQueryOrderByDesc(syntheticWorkload.getRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query order by time desc because ", e);
          }
          break;
        case VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC:
          try {
            dbWrapper.valueRangeQueryOrderByDesc(syntheticWorkload.getValueRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query order by time desc because ", e);
          }
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }
      if (config.getOP_INTERVAL() > 0) {
        long elapsed = System.currentTimeMillis() - start;
        if (elapsed < config.getOP_INTERVAL()) {
          try {
            Thread.sleep(config.getOP_INTERVAL() - elapsed);
          } catch (InterruptedException e) {
            LOGGER.error("Wait for next operation failed because ", e);
          }
        }
      }
    }
  }

  /**
   * Do Ingestion Operation
   *
   * @param actualDeviceFloor @Return when connect failed return false
   */
  private boolean ingestionOperation(double actualDeviceFloor) {
    if (config.isIS_CLIENT_BIND()) {
      if (config.isIS_SENSOR_TS_ALIGNMENT()) {
        // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
        try {
          List<DeviceSchema> schemas = baseDataSchema.getThreadDeviceSchema(clientThreadId);
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() < actualDeviceFloor) {
              Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
              dbWrapper.insertOneBatch(batch);
            }
          }
        } catch (DBConnectException e) {
          LOGGER.error("Failed to insert one batch data because ", e);
          return false;
        } catch (Exception e) {
          LOGGER.error("Failed to insert one batch data because ", e);
        }
        insertLoopIndex++;
      } else {
        // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
        try {
          List<DeviceSchema> schemas = baseDataSchema.getThreadDeviceSchema(clientThreadId);
          DeviceSchema sensorSchema = null;
          List<String> sensorList = new ArrayList<String>();
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() < actualDeviceFloor) {
              for (String sensor : deviceSchema.getSensors()) {
                int colIndex = Integer.parseInt(sensor.replace(Constants.SENSOR_NAME_PREFIX, ""));
                sensorList = new ArrayList<String>();
                sensorList.add(sensor);
                sensorSchema = (DeviceSchema) deviceSchema.clone();
                sensorSchema.setSensors(sensorList);
                Batch batch =
                    syntheticWorkload.getOneBatch(sensorSchema, insertLoopIndex, colIndex);
                batch.setColIndex(colIndex);
                Type colType = baseDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
                batch.setColType(colType);
                dbWrapper.insertOneSensorBatch(batch);
                insertLoopIndex++;
              }
            }
          }
        } catch (DBConnectException e) {
          LOGGER.error("Failed to insert one batch data because ", e);
          return false;
        } catch (Exception e) {
          LOGGER.error("Failed to insert one batch data because ", e);
        }
      }
    } else {
      // IS_CLIENT_BIND = false
      // not in use
      try {
        Batch batch = singletonWorkload.getOneBatch();
        if (batch.getDeviceSchema().getDeviceId() < actualDeviceFloor) {
          dbWrapper.insertOneBatch(batch);
        }
      } catch (DBConnectException e) {
        LOGGER.error("Failed to insert one batch data because ", e);
        return false;
      } catch (Exception e) {
        LOGGER.error("Failed to insert one batch data because ", e);
      }
    }
    return true;
  }
}
