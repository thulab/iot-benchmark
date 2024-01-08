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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.xml;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "sensor")
public class SensorXml {
  private String name;
  private String type;
  private String bindFunction;
  private String bindInterval;

  @XmlAttribute(name = "name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @XmlAttribute(name = "type")
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getBindFunction() {
    return bindFunction;
  }

  public void setBindFunction(String bindFunction) {
    this.bindFunction = bindFunction;
  }

  public String getBindInterval() {
    return bindInterval;
  }

  public void setBindInterval(String bindInterval) {
    this.bindInterval = bindInterval;
  }
}
