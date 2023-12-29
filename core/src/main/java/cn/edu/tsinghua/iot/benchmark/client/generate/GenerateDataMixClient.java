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

package cn.edu.tsinghua.iot.benchmark.client.generate;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.client.operation.OperationController;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class GenerateDataMixClient extends GenerateBaseClient {

  /** Control operation according to OPERATION_PROPORTION */
  private final OperationController operationController;

  private final Random random = new Random(config.getDATA_SEED() + clientThreadId);

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
      if (config.getOP_MIN_INTERVAL() > 0) {
        start = System.currentTimeMillis();
      }
      if (operation == Operation.INGESTION) {
        if (!ingestionOperation()) {
          break;
        }
      } else {
        if (config.isIS_RECENT_QUERY()) {
          long timestamp = dataWorkLoad.getCurrentTimestamp();
          if (!config.isIS_QUIET_MODE()) {
            String currentThread = Thread.currentThread().getName();
            LOGGER.info(
                "{} update queryWorkLoad with maxTimestamp : {}.", currentThread, timestamp);
          }
          queryWorkLoad.updateTime(timestamp);
        }
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
            case GROUP_BY_QUERY_ORDER_BY_TIME_DESC:
              dbWrapper.groupByQueryOrderByDesc(queryWorkLoad.getGroupByQuery());
              break;
            default:
              LOGGER.error("Unsupported operation sensorType {}", operation);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to do " + operation.getName() + " query because ", e);
        }
      }
      if (isStop.get()) {
        break;
      }
      if (config.getOP_MIN_INTERVAL() > 0) {
        long opMinInterval;
        if (config.isOP_MIN_INTERVAL_RANDOM()) {
          opMinInterval = (long) (random.nextDouble() * config.getOP_MIN_INTERVAL());
        } else {
          opMinInterval = config.getOP_MIN_INTERVAL();
        }
        long elapsed = System.currentTimeMillis() - start;
        if (elapsed < opMinInterval) {
          try {
            LOGGER.debug("[Client-{}] sleep {} ms.", clientThreadId, opMinInterval - elapsed);
            Thread.sleep(opMinInterval - elapsed);
          } catch (InterruptedException e) {
            LOGGER.error("Wait for next operation failed because ", e);
          }
        }
      }
    }
  }

  /** Do Ingestion Operation @Return when connect failed return false */
  private boolean ingestionOperation() {
    try {
      for (int i = 0; i < clientDeviceSchemas.size(); i += config.getDEVICE_NUM_PER_WRITE()) {
        int innerLoop = 0;
        if (config.isIS_SENSOR_TS_ALIGNMENT()) {
          innerLoop = 1;
        } else {
          if (config.isIS_CLIENT_BIND()) {
            innerLoop = clientDeviceSchemas.get(i).getSensors().size();
          } else {
            innerLoop = clientDeviceSchemas.get(i).getSensors().size() * config.getDEVICE_NUMBER();
          }
        }
        for (int j = 0; j < innerLoop; j++) {
          if (isStop.get()) {
            return true;
          }
          IBatch batch = dataWorkLoad.getOneBatch();
          if (checkBatch(batch)) {
            dbWrapper.insertOneBatchWithCheck(batch);
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
