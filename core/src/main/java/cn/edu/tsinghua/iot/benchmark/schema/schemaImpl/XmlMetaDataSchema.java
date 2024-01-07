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

package cn.edu.tsinghua.iot.benchmark.schema.schemaImpl;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iot.benchmark.function.FunctionXml;
import cn.edu.tsinghua.iot.benchmark.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class XmlMetaDataSchema extends MetaDataSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(XmlMetaDataSchema.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean createMetaDataSchema() {
    SchemaXml xml = null;
    String configFolder = System.getProperty(Constants.BENCHMARK_CONF, "configuration/conf");
    try {
      InputStream input = Files.newInputStream(Paths.get(configFolder + "/schema.xml"));
      JAXBContext context = JAXBContext.newInstance(SchemaXml.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      xml = (SchemaXml) unmarshaller.unmarshal(input);
    } catch (Exception e) {
      LOGGER.error("Failed to load function xml", e);
      System.exit(0);
    }
    // TODO spricoder
    return false;
  }
}
