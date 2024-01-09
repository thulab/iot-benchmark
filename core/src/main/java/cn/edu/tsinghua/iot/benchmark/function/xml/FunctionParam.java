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

package cn.edu.tsinghua.iot.benchmark.function.xml;

import cn.edu.tsinghua.iot.benchmark.function.enums.FunctionType;
import cn.edu.tsinghua.iot.benchmark.utils.ReadWriteIOUtils;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

public class FunctionParam {
  /** id of function */
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

  private FunctionBaseLine baseLine;

  @XmlAttribute(name = "type")
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

  @XmlElement(name = "baseLine")
  public FunctionBaseLine getBaseLine(){return baseLine;}

  public void setBaseLine(FunctionBaseLine baseLine){this.baseLine = baseLine;}



  public FunctionParam(String functionType, double max, double min) {
    super();
    this.functionType = functionType;
    this.max = max;
    this.min = min;
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
    if(baseLine!=null){
      return "FunctionParam [id="
              + id
              + ", functionType="
              + functionType
              + ", max="
              + max
              + ", min="
              + min
              + ", FunctionBaseLine="
              + baseLine.toString()
              + "]";
    }
    return "FunctionParam [id="
            + id
            + ", functionType="
            + functionType
            + ", max="
            + max
            + ", min="
            + min
            + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FunctionParam) {
      return Objects.equals(getId(), ((FunctionParam) obj).getId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, functionType, max, min, baseLine);
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(id, outputStream);
    ReadWriteIOUtils.write(functionType, outputStream);
    ReadWriteIOUtils.write(max, outputStream);
    ReadWriteIOUtils.write(min, outputStream);
    if(baseLine != null){
      baseLine.serialize(outputStream);
    }
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static FunctionParam deserialize(ByteArrayInputStream inputStream) throws IOException {
    FunctionParam result = new FunctionParam();
    result.id = ReadWriteIOUtils.readString(inputStream);
    result.functionType = ReadWriteIOUtils.readString(inputStream);
    result.max = ReadWriteIOUtils.readDouble(inputStream);
    result.min = ReadWriteIOUtils.readDouble(inputStream);
    result.baseLine = FunctionBaseLine.deserialize(inputStream);
    return result;
  }
}
