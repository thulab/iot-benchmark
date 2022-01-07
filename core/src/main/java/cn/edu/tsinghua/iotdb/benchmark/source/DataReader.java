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

package cn.edu.tsinghua.iotdb.benchmark.source;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;

import java.util.List;

public abstract class DataReader {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected final List<String> files;
  protected int currentFileIndex = 0;
  protected String currentFileName;

  public static DataReader getInstance(List<String> files) {
    if (config.isIS_COPY_MODE()) {
      return new CopyDataReader(files);
    } else {
      return new CSVDataReader(files);
    }
  }

  public DataReader(List<String> files) {
    this.files = files;
  }

  /** check whether it has next batch */
  public abstract boolean hasNextBatch();

  /** convert the cachedLines to Record list */
  public abstract Batch nextBatch();
}
