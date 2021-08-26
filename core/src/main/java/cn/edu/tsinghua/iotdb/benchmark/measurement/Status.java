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

package cn.edu.tsinghua.iotdb.benchmark.measurement;

import java.util.List;

public class Status {

  /** Whether is ok */
  private final boolean isOk;
  /** The cost time of query */
  private long costTime;
  /** The result point of query */
  private int queryResultPointNum;
  /** The exception occurred */
  private Exception exception;
  /** errorMessage is our self-defined message used to logged, it can be error SQL or anything */
  private String errorMessage;
  /** SQL */
  private String sql;
  /** results */
  private List<List<String>> records;

  public Status(boolean isOk) {
    this.isOk = isOk;
  }

  public Status(boolean isOk, int queryResultPointNum) {
    this.isOk = isOk;
    this.queryResultPointNum = queryResultPointNum;
  }

  public Status(boolean isOk, int queryResultPointNum, String sql, List<List<String>> records) {
    this.isOk = isOk;
    this.queryResultPointNum = queryResultPointNum;
    this.sql = sql;
    this.records = records;
  }

  public Status(boolean isOk, Exception exception, String errorMessage) {
    this.isOk = isOk;
    this.exception = exception;
    this.errorMessage = errorMessage;
  }

  public Status(boolean isOk, int queryResultPointNum, Exception exception, String errorMessage) {
    this.isOk = isOk;
    this.exception = exception;
    this.errorMessage = errorMessage;
    this.queryResultPointNum = queryResultPointNum;
  }

  public int getQueryResultPointNum() {
    return queryResultPointNum;
  }

  public long getTimeCost() {
    return costTime;
  }

  public void setTimeCost(long costTime) {
    this.costTime = costTime;
  }

  public Exception getException() {
    return exception;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public List<List<String>> getRecords() {
    return records;
  }

  public String getSql() {
    return sql;
  }

  public boolean isOk() {
    return isOk;
  }
}
