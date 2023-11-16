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

package cn.edu.tsinghua.iot.benchmark.workload;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.workload.interfaces.IDataWorkLoad;

import java.util.List;

public abstract class DataWorkLoad implements IDataWorkLoad {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  protected static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
  protected long currentTimestamp = 0;

  public static IDataWorkLoad getInstance(int clientId) {
    if (config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_WRITE
        || config.getBENCHMARK_WORK_MODE() == BenchmarkMode.VERIFICATION_QUERY) {
      List<String> files = MetaUtil.getClientFiles().get(clientId);
      return new RealDataWorkLoad(files);
    } else {
      if (config.isIS_CLIENT_BIND()) {
        List<DeviceSchema> deviceSchemas = metaDataSchema.getDeviceSchemaByClientId(clientId);
        return new SyntheticDataWorkLoad(deviceSchemas);
      } else {
        return SingletonWorkDataWorkLoad.getInstance();
      }
    }
  }

  @Override
  public long getCurrentTimestamp() {
    return currentTimestamp;
  }
}
