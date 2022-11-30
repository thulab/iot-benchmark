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

import cn.edu.tsinghua.iot.benchmark.function.enums.FunctionType;

import javax.xml.bind.annotation.XmlAttribute;

public class FunctionParam {
  /** Id of function */
  private String id;
  /**
   * Type of function
   *
   * @see FunctionType
   */
  private String functionType;
  /** Maximum of function */
  private double max;
  /** Minimum of function */
  private double min;
  /** Cycle of function For *-k function, only be used to calculate k */
  private long cycle;

  @XmlAttribute(name = "function-sensorType")
  public String getFunctionType() {
    return functionType;
  }

  public void setFunctionType(String functionType) {
    this.functionType = functionType;
  }

  @XmlAttribute(name = "max")
  public double getMax() {
    return max;
  }

  public void setMax(double max) {
    this.max = max;
  }

  @XmlAttribute(name = "min")
  public double getMin() {
    return min;
  }

  public void setMin(double min) {
    this.min = min;
  }

  @XmlAttribute(name = "cycle")
  public long getCycle() {
    return cycle;
  }

  public void setCycle(long cycle) {
    this.cycle = cycle;
  }

  public FunctionParam(String functionType, double max, double min, long cycle) {
    super();
    this.functionType = functionType;
    this.max = max;
    this.min = min;
    this.cycle = cycle;
  }

  public FunctionParam() {
    super();
  }

  @XmlAttribute(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "FunctionParam [id="
        + id
        + ", functionType="
        + functionType
        + ", max="
        + max
        + ", min="
        + min
        + ", cycle="
        + cycle
        + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (getId().equals(((FunctionParam) obj).getId())) {
      return true;
    }
    return super.equals(obj);
  }
}
