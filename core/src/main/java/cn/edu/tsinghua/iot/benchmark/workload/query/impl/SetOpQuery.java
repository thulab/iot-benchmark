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

package cn.edu.tsinghua.iot.benchmark.workload.query.impl;

import java.util.List;

public class SetOpQuery extends Query {

  private List<RangeQuery> childRangeQueries;

  private String setOpType;

  public SetOpQuery() {}

  public SetOpQuery(List<RangeQuery> childRangeQueries, String setOpType) {
    this.childRangeQueries = childRangeQueries;
    this.setOpType = setOpType;
  }

  public List<RangeQuery> getChildRangeQueries() {
    return childRangeQueries;
  }

  public void setChildRangeQueries(List<RangeQuery> childRangeQueries) {
    this.childRangeQueries = childRangeQueries;
  }

  public String getSetOpType() {
    return setOpType;
  }

  public void setSetOpType(String setOpType) {
    this.setOpType = setOpType;
  }

  @Override
  public StringBuilder getQueryAttrs() {

    StringBuilder resultBuilder = new StringBuilder();
    for (RangeQuery childRangeQuery : childRangeQueries) {
      resultBuilder
          .append("childQueryAttrs=(")
          .append(childRangeQuery.getQueryAttrs())
          .append(") ");
    }
    resultBuilder.append("setOpType=").append(setOpType);

    return resultBuilder;
  }
}
