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
import cn.edu.tsinghua.iotdb.benchmark.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.schema.enums.Type;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;

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
  protected void doOperations(int actualDeviceFloor) {
    long start = 0;
    for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
      Operation operation = operationController.getNextOperationType();
      if (config.getOP_INTERVAL() > 0) {
        start = System.currentTimeMillis();
      }
      if (operation == Operation.INGESTION) {
        if (!ingestionOperation(actualDeviceFloor)) {
          break;
        }
      } else {
        try {
          for (DBWrapper dbWrapper : dbWrappers) {
            switch (operation) {
              case PRECISE_QUERY:
                dbWrapper.preciseQuery(syntheticWorkload.getPreciseQuery());
                break;
              case RANGE_QUERY:
                dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
                break;
              case VALUE_RANGE_QUERY:
                dbWrapper.valueRangeQuery(syntheticWorkload.getValueRangeQuery());
                break;
              case AGG_RANGE_QUERY:
                dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
                break;
              case AGG_VALUE_QUERY:
                dbWrapper.aggValueQuery(syntheticWorkload.getAggValueQuery());
                break;
              case AGG_RANGE_VALUE_QUERY:
                dbWrapper.aggRangeValueQuery(syntheticWorkload.getAggRangeValueQuery());
                break;
              case GROUP_BY_QUERY:
                dbWrapper.groupByQuery(syntheticWorkload.getGroupByQuery());
                break;
              case LATEST_POINT_QUERY:
                dbWrapper.latestPointQuery(syntheticWorkload.getLatestPointQuery());
                break;
              case RANGE_QUERY_ORDER_BY_TIME_DESC:
                dbWrapper.rangeQueryOrderByDesc(syntheticWorkload.getRangeQuery());
                break;
              case VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC:
                dbWrapper.valueRangeQueryOrderByDesc(syntheticWorkload.getValueRangeQuery());
                break;
              default:
                LOGGER.error("Unsupported operation type {}", operation);
            }
          }
        } catch (Exception e) {
          LOGGER.error("Failed to do " + operation.getName() + " query because ", e);
        }
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
  private boolean ingestionOperation(int actualDeviceFloor) {
    try {
      if (config.isIS_CLIENT_BIND()) {
        List<DeviceSchema> schemas = baseDataSchema.getThreadDeviceSchema(clientThreadId);
        if (config.isIS_SENSOR_TS_ALIGNMENT()) {
          // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() <= actualDeviceFloor) {
              Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
              for (DBWrapper dbWrapper : dbWrappers) {
                dbWrapper.insertOneBatch(batch);
              }
            }
          }
          insertLoopIndex++;
        } else {
          // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
          DeviceSchema sensorSchema = null;
          List<String> sensorList = new ArrayList<String>();
          for (DeviceSchema deviceSchema : schemas) {
            if (deviceSchema.getDeviceId() <= actualDeviceFloor) {
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
                for (DBWrapper dbWrapper : dbWrappers) {
                  dbWrapper.insertOneSensorBatch(batch);
                }
                insertLoopIndex++;
              }
            }
          }
        }
      } else {
        // IS_CLIENT_BIND = false
        Batch batch = singletonWorkload.getOneBatch();
        if (batch.getDeviceSchema().getDeviceId() <= actualDeviceFloor) {
          for (DBWrapper dbWrapper : dbWrappers) {
            dbWrapper.insertOneBatch(batch);
          }
        }
      }
    } catch (DBConnectException e) {
      LOGGER.error("Failed to insert one batch data because ", e);
      return false;
    } catch (Exception e) {
      LOGGER.error("Failed to insert one batch data because ", e);
    }
    return true;
  }
}
