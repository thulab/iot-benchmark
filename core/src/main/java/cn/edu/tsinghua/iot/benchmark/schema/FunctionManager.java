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

package cn.edu.tsinghua.iot.benchmark.schema;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.function.xml.FunctionParam;
import cn.edu.tsinghua.iot.benchmark.function.xml.FunctionXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FunctionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManager.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  /** Built-in function parameters */
  private final List<FunctionParam> LINE_LIST = new ArrayList<>();

  private final List<FunctionParam> SIN_LIST = new ArrayList<>();
  private final List<FunctionParam> SQUARE_LIST = new ArrayList<>();
  private final List<FunctionParam> RANDOM_LIST = new ArrayList<>();
  private final Map<String, FunctionParam> FUNCTION_MAP = new ConcurrentHashMap<>();

  private FunctionManager() {
    // TODO spricoder optimize
    FunctionXml xml = null;
    String configFolder = System.getProperty(Constants.BENCHMARK_CONF, "configuration/conf");
    try {
      InputStream input = Files.newInputStream(Paths.get(configFolder + "/function.xml"));
      JAXBContext context = JAXBContext.newInstance(FunctionXml.class, FunctionParam.class);
      Unmarshaller unmarshaller = context.createUnmarshaller();
      xml = (FunctionXml) unmarshaller.unmarshal(input);
    } catch (Exception e) {
      LOGGER.error("Failed to load function xml", e);
      System.exit(0);
    }
    List<FunctionParam> xmlFunctions = xml.getFunctions();
    for (FunctionParam param : xmlFunctions) {
      if (param.getFunctionType().contains("mono")) {
        LINE_LIST.add(param);
      } else if (param.getFunctionType().contains("sin")) {
        SIN_LIST.add(param);
      } else if (param.getFunctionType().contains("square")) {
        SQUARE_LIST.add(param);
      } else if (param.getFunctionType().contains("random")) {
        RANDOM_LIST.add(param);
      }
      FUNCTION_MAP.put(param.getId(), param);
    }
  }

  public static class FunctionManagerHolder {
    private static final FunctionManager INSTANCE = new FunctionManager();
  }

  public static FunctionManager getInstance() {
    return FunctionManagerHolder.INSTANCE;
  }

  public FunctionParam getById(String id) {
    return FUNCTION_MAP.get(id);
  }

  public List<FunctionParam> getLINE_LIST() {
    return LINE_LIST;
  }

  public List<FunctionParam> getSIN_LIST() {
    return SIN_LIST;
  }

  public List<FunctionParam> getSQUARE_LIST() {
    return SQUARE_LIST;
  }

  public List<FunctionParam> getRANDOM_LIST() {
    return RANDOM_LIST;
  }

}
