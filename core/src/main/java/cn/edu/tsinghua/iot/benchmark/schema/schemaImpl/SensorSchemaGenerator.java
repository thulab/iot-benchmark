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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iot.benchmark.function.FunctionXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SensorSchemaGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SensorSchemaGenerator.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Built-in function parameters */
  private final List<FunctionParam> LINE_LIST = new ArrayList<>();

  private final List<FunctionParam> SIN_LIST = new ArrayList<>();
  private final List<FunctionParam> SQUARE_LIST = new ArrayList<>();
  private final List<FunctionParam> RANDOM_LIST = new ArrayList<>();
  private final List<FunctionParam> CONSTANT_LIST = new ArrayList<>();

  private static class FunctionManagerHolder {
    private static final SensorSchemaGenerator INSTANCE = new SensorSchemaGenerator();
  }

  private SensorSchemaGenerator() {
    FunctionXml xml = null;
    String configFolder = System.getProperty(Constants.BENCHMARK_CONF, "configuration/conf");
    try {
      InputStream input = Files.newInputStream(Paths.get(configFolder + "/function.xml"));
      JAXBContext context = JAXBContext.newInstance(FunctionXml.class, FunctionParam.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      xml = (FunctionXml) unmarshaller.unmarshal(input);
    } catch (Exception e) {
      LOGGER.error("Failed to load function xml", e);
      System.exit(0);
    }
    List<FunctionParam> xmlFunctions = xml.getFunctions();
    for (FunctionParam param : xmlFunctions) {
      if (param.getFunctionType().contains("_mono_k")) {
        LINE_LIST.add(param);
      } else if (param.getFunctionType().contains("_mono")) {
        // if min equals to max, then it is constant.
        if (param.getMin() == param.getMax()) {
          CONSTANT_LIST.add(param);
        }
      } else if (param.getFunctionType().contains("_sin")) {
        SIN_LIST.add(param);
      } else if (param.getFunctionType().contains("_square")) {
        SQUARE_LIST.add(param);
      } else if (param.getFunctionType().contains("_random")) {
        RANDOM_LIST.add(param);
      }
    }
  }

  public static SensorSchemaGenerator getInstance() {
    return FunctionManagerHolder.INSTANCE;
  }

  /** According to the number of sensors, generate sensor */
  public List<Sensor> generateSensor() {
    int TYPE_NUMBER = 6;
    double CONSTANT_RATIO = config.getCONSTANT_RATIO();
    double LINE_RATIO = config.getLINE_RATIO();
    double RANDOM_RATIO = config.getRANDOM_RATIO();
    double SIN_RATIO = config.getSIN_RATIO();
    double SQUARE_RATIO = config.getSQUARE_RATIO();
    // Configure according to the ratio of each function passed in
    double sumRatio = CONSTANT_RATIO + LINE_RATIO + RANDOM_RATIO + SIN_RATIO + SQUARE_RATIO;
    // Check whether the configuration is correct
    if (sumRatio == 0
        || CONSTANT_RATIO < 0
        || LINE_RATIO < 0
        || RANDOM_RATIO < 0
        || SIN_RATIO < 0
        || SQUARE_RATIO < 0) {
      System.err.println("function ration must >= 0 and sum > 0");
      System.exit(0);
    }
    List<Sensor> sensors = new ArrayList<>();
    Random r = new Random(config.getDATA_SEED());
    double constantArea = CONSTANT_RATIO / sumRatio;
    double lineArea = constantArea + LINE_RATIO / sumRatio;
    double randomArea = lineArea + RANDOM_RATIO / sumRatio;
    double sinArea = randomArea + SIN_RATIO / sumRatio;
    double squareArea = sinArea + SQUARE_RATIO / sumRatio;
    // Generate Probabilities of sensors
    double[] probabilities = generateProbabilities(TYPE_NUMBER);
    if (probabilities == null) {
      return sensors;
    }
    for (int sensorIndex = 0; sensorIndex < config.getSENSOR_NUMBER(); sensorIndex++) {
      double sensorPosition = (sensorIndex + 1) * 1.0 / config.getSENSOR_NUMBER();
      int i;
      for (i = 1; i <= TYPE_NUMBER; i++) {
        if (sensorPosition > probabilities[i - 1] && sensorPosition <= probabilities[i]) {
          break;
        }
      }
      double property = r.nextDouble();
      FunctionParam param = null;
      Random fr = new Random(config.getDATA_SEED() + 1 + i);
      double middle = fr.nextDouble();
      // constant
      if (property < constantArea) {
        int index = (int) (middle * CONSTANT_LIST.size());
        param = CONSTANT_LIST.get(index);
      }
      // line
      if (property >= constantArea && property < lineArea) {
        int index = (int) (middle * LINE_LIST.size());
        param = LINE_LIST.get(index);
      }
      // random
      if (property >= lineArea && property < randomArea) {
        int index = (int) (middle * RANDOM_LIST.size());
        param = RANDOM_LIST.get(index);
      }
      // sin
      if (property >= randomArea && property < sinArea) {
        int index = (int) (middle * SIN_LIST.size());
        param = SIN_LIST.get(index);
      }
      // square
      if (property >= sinArea && property < squareArea) {
        int index = (int) (middle * SQUARE_LIST.size());
        param = SQUARE_LIST.get(index);
      }
      if (param == null) {
        System.err.println(
            "There is a problem with the initialization function scale "
                + "in initSensorFunction()!");
        System.exit(0);
      }
      Sensor sensor =
          new Sensor(
              config.getSENSOR_NAME_PREFIX() + sensorIndex, SensorType.getType(i - 1), param);
      sensors.add(sensor);
    }
    return sensors;
  }

  /** Generate Probabilities according to proportion(e.g. 1:1:1:1:1:1) */
  private double[] generateProbabilities(int TYPE_NUMBER) {
    // Probabilities for Types
    double[] probabilities = new double[TYPE_NUMBER + 1];
    // Origin proportion array
    double[] proportions = new double[TYPE_NUMBER];
    // unified proportion array
    List<Double> proportion = new ArrayList<>();
    String INSERT_DATATYPE_PROPORTION = config.getINSERT_DATATYPE_PROPORTION();
    LOGGER.info(
        "Init SensorTypes: BOOLEAN:INT32:INT64:FLOAT:DOUBLE:TEXT=" + INSERT_DATATYPE_PROPORTION);

    String[] split = INSERT_DATATYPE_PROPORTION.split(":");
    if (split.length != TYPE_NUMBER) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
      return null;
    }
    double sum = 0;
    for (int i = 0; i < TYPE_NUMBER; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < TYPE_NUMBER; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
      }
    }
    probabilities[0] = 0.0;
    for (int i = 1; i <= TYPE_NUMBER; i++) {
      probabilities[i] = probabilities[i - 1] + proportion.get(i - 1);
    }
    return probabilities;
  }
}
