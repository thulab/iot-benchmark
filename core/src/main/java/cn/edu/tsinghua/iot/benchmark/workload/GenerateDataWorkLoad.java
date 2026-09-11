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

package cn.edu.tsinghua.iot.benchmark.workload;

import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iot.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.function.Function;
import cn.edu.tsinghua.iot.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public abstract class GenerateDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataWorkLoad.class);

  private static final Random poissonRandom = new Random(config.getDATA_SEED());
  private static final PoissonDistribution poissonDistribution =
      new PoissonDistribution(poissonRandom);
  private static final Random dataRandom = new Random(config.getDATA_SEED());
  // Seed for the sparse matrix write (NULL_RATIO) null pattern.
  private static final long NULL_SEED = config.getDATA_SEED();
  private static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final long timeStampConst =
      TimeUtils.getTimestampConst(config.getTIMESTAMP_PRECISION());

  /**
   * workloadValues[SENSOR_NUMBER][WORKLOAD_BUFFER_SIZE]。 For those regular data, a piece of data of
   * each sensor is stored for rapid generation according to the law this must after timeStampConst
   */
  private static final Object[][] workloadValues = initWorkloadValues();

  private static final long OUT_OF_ORDER_BASE =
      (long) (config.getLOOP() * config.getOUT_OF_ORDER_RATIO());
  private final ProbTool probTool = new ProbTool();
  protected int deviceSchemaSize = 0;

  @Override
  public long getBatchNumber() {
    return config.getDEVICE_NUMBER() * config.getLOOP();
  }

  /** Add one row into batch, row contains data from all sensors */
  protected List<Object> generateOneRow(int deviceIndex, int colIndex, long stepOffset)
      throws WorkloadException {
    List<Object> values = new ArrayList<>(config.getSENSOR_NUMBER());
    int index =
        (int)
            Math.floorMod(stepOffset * (deviceIndex + 1), (long) config.getWORKLOAD_BUFFER_SIZE());
    if (colIndex == -1) {
      for (int i = 0; i < config.getSENSOR_NUMBER(); i++) {
        values.add(workloadValues[i][index]);
      }
    } else {
      values.add(workloadValues[colIndex][index]);
    }
    // Sparse matrix write: each cell is null with probability NULL_RATIO, independently. The
    // values list is a fresh copy, so setting null here never mutates the shared static
    // workloadValues.
    if (config.getNULL_RATIO() > 0) {
      for (int i = 0; i < values.size(); i++) {
        // When IS_SENSOR_TS_ALIGNMENT is false the row holds a single column, whose index is the
        // cursor carried by colIndex rather than the loop index.
        int columnIndex = colIndex == -1 ? i : colIndex;
        if (isNullCell(deviceIndex, stepOffset, columnIndex)) {
          values.set(i, null);
        }
      }
    }
    return values;
  }

  /**
   * Decides whether the cell identified by {@code (deviceId, stepOffset, columnIndex)} is null.
   *
   * <p>The decision is derived from {@code DATA_SEED} and the cell coordinates instead of from a
   * shared {@link Random}, so it does not depend on the order in which data clients consume random
   * numbers: the same cell is null (or not) in every run, no matter which thread generates it. This
   * matters because {@link SingletonWorkDataWorkLoad} is shared across data client threads when
   * {@code IS_CLIENT_BIND=false}.
   *
   * <p>This method is pure, so it needs no synchronization.
   */
  private static boolean isNullCell(long deviceId, long stepOffset, int columnIndex) {
    // Mix the coordinates into the seed, then apply the murmur3 finalizer so that neighbouring
    // coordinates do not yield correlated decisions.
    long hash = NULL_SEED;
    hash = hash * 31 + deviceId;
    hash = hash * 31 + stepOffset;
    hash = hash * 31 + columnIndex;
    hash ^= hash >>> 33;
    hash *= 0xff51afd7ed558ccdL;
    hash ^= hash >>> 33;
    hash *= 0xc4ceb9fe1a85ec53L;
    hash ^= hash >>> 33;
    // The high 24 bits are the best mixed; map them onto [0, 1).
    return (hash >>> 40) / (double) (1 << 24) < config.getNULL_RATIO();
  }

  /** Get timestamp according to stepOffset */
  protected long getCurrentTimestamp(long stepOffset) throws WorkloadException {
    if (config.isIS_OUT_OF_ORDER()) {
      // change offset according to out of order mode
      switch (config.getOUT_OF_ORDER_MODE()) {
        case POISSON:
          if (probTool.returnTrueByProb(config.getOUT_OF_ORDER_RATIO(), poissonRandom)) {
            stepOffset -= poissonDistribution.getNextPoissonDelta();
          }
          break;
        case BATCH:
          stepOffset = (stepOffset + OUT_OF_ORDER_BASE) % config.getLOOP();
          break;
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }

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
      timestamp += config.getPOINT_STEP() * ThreadLocalRandom.current().nextDouble();
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
      timestamp += config.getPOINT_STEP() * ThreadLocalRandom.current().nextDouble();
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
    if (config.hasWrite()) {
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
            case TIMESTAMP:
              value = number.longValue();
              break;
            case FLOAT:
              value = (float) (Math.round(number.floatValue() * ratio) / ratio);
              break;
            case DOUBLE:
              value = Math.round(number.doubleValue() * ratio) / ratio;
              break;
            case TEXT:
            case STRING:
            case BLOB:
              StringBuffer builder = new StringBuffer(config.getSTRING_LENGTH());
              for (int k = 0; k < config.getSTRING_LENGTH(); k++) {
                builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
              }
              value = builder.toString();
              break;
            case OBJECT:
              byte[] object = new byte[config.getOBJECT_LENGTH()];
              dataRandom.nextBytes(object);
              value = object;
              break;
            case DATE:
              value = LocalDate.ofEpochDay(number.shortValue());
              break;
            default:
              throw new UnsupportedOperationException(
                  sensor.getSensorType() + ": This data type is not supported.");
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
