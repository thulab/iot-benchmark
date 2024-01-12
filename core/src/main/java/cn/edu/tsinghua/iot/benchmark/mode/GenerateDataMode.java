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

package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.extern.SchemaWriter;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateDataMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();

  @Override
  protected boolean preCheck() {
    return SchemaWriter.getBasicWriter().writeSchema(metaDataSchema.getAllDeviceSchemas());
  }

  @Override
  protected void postCheck() {
    LOGGER.info("Data Location: {}", config.getFILE_PATH());
    LOGGER.info("Schema Location: {}", FileUtils.union(config.getFILE_PATH(), "schema.txt"));
    LOGGER.info("Generate Info Location: {}", FileUtils.union(config.getFILE_PATH(), "info.txt"));
  }
}
