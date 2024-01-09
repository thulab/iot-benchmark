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

package cn.edu.tsinghua.iot.benchmark.function;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.function.enums.FunctionType;
import cn.edu.tsinghua.iot.benchmark.function.xml.FunctionBaseLine;
import cn.edu.tsinghua.iot.benchmark.function.xml.FunctionParam;

import java.util.Random;

public class Function {

  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  /** use DATA_SEED in config */
  private static final Random random = new Random(config.getDATA_SEED());

  private Function(){}

  /** Get value of function */
  public static Number getValueByFunctionIdAndParam(FunctionParam param, long currentTime) {
    return getValueByFunctionIdAndParam(
        FunctionType.valueOf(param.getFunctionType().toUpperCase()),
        param.getMax(),
        param.getMin(),
        param.getBaseLine(),
        random.nextInt(20000),
        currentTime);
  }

  private static Number getValueByFunctionIdAndParam(
          FunctionType functionType, double max, double min, FunctionBaseLine baseLine,int cycle,long currentTime) {
    switch (functionType) {
      case SIN:
        return (float) getSineValue(max, min, baseLine,cycle, currentTime);
      case RANDOM:
        return (float) getRandomValue(max, min, baseLine);
      case SQUARE:
        return (float) getSquareValue(max, min, baseLine, cycle,currentTime);
      case MONO:
        return (float) getMonoValue(max, min, baseLine, cycle,currentTime);
      default:
        return 0;
    }
  }

  /**
   * Get value of monotonic function
   *
   * @param max maximum of function
   * @param min minimum of function
   * @param cycle time unit is ms
   * @param currentTime time unit is ms
   * @return mono value
   */
  private static double getMonoValue(double max, double min, FunctionBaseLine baseLine,double cycle, long currentTime) {
    double k = (max - min) / cycle;
    return min + k * (currentTime % cycle);
  }

  /**
   * Get value of sin function
   *
   * @param max maximum of function
   * @param min minimum of function
   * @param cycle time unit is ms
   * @param currentTime time unit is ms
   * @return sin value
   */
  private static double getSineValue(double max, double min, FunctionBaseLine baseLine,double cycle, long currentTime) {
    double w = 2 * Math.PI / (cycle * 1000);
    double a = (max - min) / 2;
    double b = (max - min) / 2;
    return Math.sin(w * (currentTime % (cycle * 1000))) * a + b + min;
  }

  /**
   * Get value of square function
   *
   * @param max maximum of function
   * @param min minimum of function
   * @param cycle time unit is ms
   * @param currentTime time unit is ms
   * @return square value
   */
  private static double getSquareValue(double max, double min, FunctionBaseLine baseLine,double cycle, long currentTime) {
    double t = cycle / 2;
    if ((currentTime % (cycle)) < t) {
      return max;
    } else {
      return min;
    }
  }

  /**
   * Get value of random function
   *
   * @param max maximum of function
   * @param min minimum of function
   * @return random value
   */
  private static double getRandomValue(double max, double min,FunctionBaseLine baseLine) {
    if(baseLine!=null){
      double probability = random.nextDouble();
        if(probability<baseLine.getRatio()){
            return random.nextDouble() * (baseLine.getUpper() - baseLine.getLower()) + baseLine.getLower();
        }
    }
    return random.nextDouble() * (max - min) + min;
  }
}
