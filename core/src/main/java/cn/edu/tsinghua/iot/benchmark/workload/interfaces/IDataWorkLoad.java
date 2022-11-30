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

package cn.edu.tsinghua.iot.benchmark.workload.interfaces;

import cn.edu.tsinghua.iot.benchmark.entity.Batch;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;

/** interface of data workload */
public interface IDataWorkLoad extends IWorkLoad {
  /** Insert one batch into database NOTICE: every row contains data from all sensors */
  Batch getOneBatch() throws WorkloadException;

  /** Get Batch Number */
  long getBatchNumber();

  /** Get recentTimestamp */
  long getCurrentTimestamp();
}
