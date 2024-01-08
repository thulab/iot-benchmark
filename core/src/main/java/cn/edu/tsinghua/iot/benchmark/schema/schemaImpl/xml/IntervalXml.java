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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "bindInterval")
public class IntervalXml {
  private String id;

  private TimeIntervalXml timeInterval;

  private WriteIntervalXml writeInterval;

  @XmlAttribute(name = "id")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @XmlElement(name = "timeInterval")
  public TimeIntervalXml getTimeInterval() {
    return timeInterval;
  }

  public void setTimeInterval(TimeIntervalXml timeInterval) {
    this.timeInterval = timeInterval;
  }

  @XmlElement(name = "writeInterval")
  public WriteIntervalXml getWriteInterval() {
    return writeInterval;
  }

  public void setWriteInterval(WriteIntervalXml writeInterval) {
    this.writeInterval = writeInterval;
  }
}
