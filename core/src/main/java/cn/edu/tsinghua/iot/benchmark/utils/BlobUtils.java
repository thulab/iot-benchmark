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

package cn.edu.tsinghua.iot.benchmark.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Formatter;

public class BlobUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlobUtils.class);

  public BlobUtils() {}

  public static String bytesToHex(byte[] bytes) {
    String hex = "";
    try (Formatter formatter = new Formatter()) {
      for (byte b : bytes) {
        formatter.format("%02X", b);
      }
      hex = formatter.toString();
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return hex;
  }

  public static String stringToHex(String input) {
    byte[] bytes = input.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return bytesToHex(bytes);
  }
}
