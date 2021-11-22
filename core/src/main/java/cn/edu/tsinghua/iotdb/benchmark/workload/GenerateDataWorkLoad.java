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

package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class GenerateDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataWorkLoad.class);

  protected static final Random poissonRandom = new Random(config.getDATA_SEED());
  protected static final Random dataRandom = new Random(config.getDATA_SEED());
  protected static final Random timestampRandom = new Random(config.getDATA_SEED());
  protected static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  protected static final long timeStampConst =
      TimeUtils.getTimestampConst(config.getTIMESTAMP_PRECISION());
  /**
   * workloadValues[SENSOR_NUMBER][WORKLOAD_BUFFER_SIZE]。 For those regular data, a piece of data of
   * each sensor is stored for rapid generation according to the law this must after timeStampConst
   */
  protected static final Object[][] workloadValues = initWorkloadValues();

  protected List<DeviceSchema> deviceSchemas = new ArrayList<>();
  protected final ProbTool probTool = new ProbTool();
  protected int deviceSchemaSize = 0;

  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (!config.isIS_OUT_OF_ORDER()) {
      return getOrderedBatch();
    } else {
      switch (config.getOUT_OF_ORDER_MODE()) {
        case POISSON:
          return getDistOutOfOrderBatch();
        case BATCH:
          return getLocalOutOfOrderBatch();
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }
  }

  @Override
  public long getBatchNumber() {
    return config.getDEVICE_NUMBER() * config.getLOOP();
  }

  protected abstract Batch getOrderedBatch();

  protected abstract Batch getDistOutOfOrderBatch();

  protected abstract Batch getLocalOutOfOrderBatch();

  /** Add one row into batch, row contains data from all sensors */
  protected void addOneRowIntoBatch(Batch batch, long stepOffset) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    if (batch.getColIndex() == -1) {
      for (int i = 0; i < config.getSENSOR_NUMBER(); i++) {
        values.add(
            workloadValues[i][(int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
      }
    } else {
      values.add(
          workloadValues[batch.getColIndex()][
              (int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    }
    batch.add(currentTimestamp, values);
  }

  /** Get timestamp according to stepOffset */
  protected long getCurrentTimestamp(long stepOffset) {
    // offset of data ahead
    long offset = config.getPOINT_STEP() * stepOffset;
    // timestamp for next data
    long timestamp = 0;
    // change timestamp frequency
    if (config.isIS_REGULAR_FREQUENCY()) {
      // data is in regular frequency, then do nothing
      timestamp += config.getPOINT_STEP();
    } else {
      // data is not in regular frequency, then use random
      timestamp += config.getPOINT_STEP() * timestampRandom.nextDouble();
    }
    long currentTimestamp = Constants.START_TIMESTAMP * timeStampConst + offset + timestamp;
    if (config.isIS_RECENT_QUERY()) {
      this.currentTimestamp = Math.max(this.currentTimestamp, currentTimestamp);
    }
    return currentTimestamp;
  }

  private static long getCurrentTimestampStatic(long stepOffset) {
    // offset of data ahead
    long offset = config.getPOINT_STEP() * stepOffset;
    // timestamp for next data
    long timestamp = 0;
    // change timestamp frequency
    if (config.isIS_REGULAR_FREQUENCY()) {
      // data is in regular frequency, then do nothing
      timestamp += config.getPOINT_STEP();
    } else {
      // data is not in regular frequency, then use random
      timestamp += config.getPOINT_STEP() * timestampRandom.nextDouble();
    }
    return Constants.START_TIMESTAMP * timeStampConst + offset + timestamp;
  }

  /** Init workload values */
  private static Object[][] initWorkloadValues() {
    double ratio = 1.0;
    for (int i = 0; i < config.getDOUBLE_LENGTH(); i++) {
      ratio *= 10;
    }
    LOGGER.info("Start Generating WorkLoad");
    Object[][] workloadValues = null;
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      int sensorNumber = config.getSENSOR_NUMBER();
      // if the first number in OPERATION_PROPORTION not equals to 0, then write data
      workloadValues = new Object[sensorNumber][config.getWORKLOAD_BUFFER_SIZE()];
      for (int sensorIndex = 0; sensorIndex < sensorNumber; sensorIndex++) {
        Sensor sensor = config.getSENSORS().get(sensorIndex);
        for (int i = 0; i < config.getWORKLOAD_BUFFER_SIZE(); i++) {
          // This time stamp is only used to generate periodic data. So the timestamp is also
          // periodic
          long currentTimestamp = getCurrentTimestampStatic(i);
          Object value;
          if (sensor.getSensorType() == SensorType.TEXT) {
            // TEXT case: pick STRING_LENGTH chars to be a String for insertion.
            StringBuffer builder = new StringBuffer(config.getSTRING_LENGTH());
            for (int k = 0; k < config.getSTRING_LENGTH(); k++) {
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          } else {
            // not TEXT case
            FunctionParam param = config.getSENSOR_FUNCTION().get(sensor.getName());
            Number number = Function.getValueByFunctionIdAndParam(param, currentTimestamp);
            switch (sensor.getSensorType()) {
              case BOOLEAN:
                value = number.floatValue() > ((param.getMax() + param.getMin()) / 2);
                break;
              case INT32:
                value = number.intValue();
                break;
              case INT64:
                value = number.longValue();
                break;
              case FLOAT:
                value = number.floatValue();
                break;
              case DOUBLE:
                value = Math.round(number.doubleValue() * ratio) / ratio;
                break;
              default:
                value = null;
                break;
            }
          }
          workloadValues[sensorIndex][i] = value;
        }
        if (sensorIndex % 5000 == 0) {
          LOGGER.info(
              "Finish {} % WorkLoad Buffer", (sensorIndex * 100.0 / config.getSENSOR_NUMBER()));
        }
      }
    } else {
      LOGGER.info("According to OPERATION_PROPORTION, there is no need to write");
    }
    LOGGER.info("Finish Generating WorkLoad");
    return workloadValues;
  }
}
