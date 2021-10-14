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
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class GenerateDataMixClient extends GenerateBaseClient {

  /** Control operation according to OPERATION_PROPORTION */
  private final OperationController operationController;

  public GenerateDataMixClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
    super(id, countDownLatch, barrier);
    // TODO exclude control model
    this.operationController = new OperationController(id);
  }

  /** Do Operations */
  @Override
  protected void doTest() {
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
          switch (operation) {
            case PRECISE_QUERY:
              dbWrapper.preciseQuery(queryWorkLoad.getPreciseQuery());
              break;
            case RANGE_QUERY:
              dbWrapper.rangeQuery(queryWorkLoad.getRangeQuery());
              break;
            case VALUE_RANGE_QUERY:
              dbWrapper.valueRangeQuery(queryWorkLoad.getValueRangeQuery());
              break;
            case AGG_RANGE_QUERY:
              dbWrapper.aggRangeQuery(queryWorkLoad.getAggRangeQuery());
              break;
            case AGG_VALUE_QUERY:
              dbWrapper.aggValueQuery(queryWorkLoad.getAggValueQuery());
              break;
            case AGG_RANGE_VALUE_QUERY:
              dbWrapper.aggRangeValueQuery(queryWorkLoad.getAggRangeValueQuery());
              break;
            case GROUP_BY_QUERY:
              dbWrapper.groupByQuery(queryWorkLoad.getGroupByQuery());
              break;
            case LATEST_POINT_QUERY:
              dbWrapper.latestPointQuery(queryWorkLoad.getLatestPointQuery());
              break;
            case RANGE_QUERY_ORDER_BY_TIME_DESC:
              dbWrapper.rangeQueryOrderByDesc(queryWorkLoad.getRangeQuery());
              break;
            case VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC:
              dbWrapper.valueRangeQueryOrderByDesc(queryWorkLoad.getValueRangeQuery());
              break;
            default:
              LOGGER.error("Unsupported operation sensorType {}", operation);
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
      for (int i = 0; i < deviceSchemas.size(); i++) {
        int innerLoop =
            config.isIS_SENSOR_TS_ALIGNMENT() ? 1 : deviceSchemas.get(i).getSensors().size();
        for (int j = 0; j < innerLoop; j++) {
          Batch batch = dataWorkLoad.getOneBatch();
          if (batch.getDeviceSchema().getDeviceId() <= actualDeviceFloor) {
            dbWrapper.insertOneBatch(batch);
          }
        }
      }
      insertLoopIndex++;
    } catch (Exception e) {
      LOGGER.error("Failed to insert one batch data because ", e);
      return false;
    }
    return true;
  }
}
